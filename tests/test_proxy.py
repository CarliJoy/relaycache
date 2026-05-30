"""
Integration tests for relaycache.

Prerequisites
-------------
    pip install pytest pytest-subtests httpx fastapi uvicorn

Run
---
    pytest tests/test_proxy.py -v

Architecture
------------
    pytest -> relaycache (subprocess) -> upstream (FastAPI + uvicorn, in-process)

Each test gets its own isolated upstream session (via the upstream fixture) so
logs and settings never bleed between tests.  The proxy fixture is unified:
TCP by default, Unix socket via @pytest.mark.proxy_settings(ProxySettings(use_unix_socket=True)).
Tests that need to restart the proxy (persistence, eviction) receive a ProxyHandle
and call handle.restart() directly.
"""

from __future__ import annotations

import dataclasses
import os
import socket
import subprocess
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import uvicorn
from upstream import Upstream
from upstream_app import BLOBS, RESOURCE_BODY, RESOURCE_ETAG
from upstream_app import app as upstream_fastapi_app

PROXY_BIN = Path(__file__).parent.parent / "target" / "release" / "relaycache"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"port {port} did not open in {timeout}s")


# ---------------------------------------------------------------------------
# ProxySettings
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ProxySettings:
    """
    Typed configuration for a relaycache process.

    Pass via @pytest.mark.proxy_settings(ProxySettings(...)) on a test or class.
    Defaults produce a standard TCP-bound proxy with a 1 h TTL.
    """

    use_unix_socket: bool = False
    entry_ttl: str = "1h"
    eviction_interval: str = "2h"
    cache_max_entries: int = 256
    max_cacheable_size: str = "512MiB"


# ---------------------------------------------------------------------------
# ProxyHandle
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ProxyHandle:
    """
    A running relaycache process together with an httpx.Client pointed at it.

    client    - use for HTTP requests
    restart() - terminate the current process, start a fresh one against the
                same cache_dir, re-point client.  Used by persistence tests.
    terminate() - shut down cleanly; called automatically by the fixture.
    """

    client: httpx.Client
    _proc: subprocess.Popen[bytes]
    _upstream_base_url: str
    _settings: ProxySettings
    _binary: Path
    _cache_dir: Path
    _tmp_path: Path

    # -- Factory -------------------------------------------------------------

    @classmethod
    def start(
        cls,
        upstream_base_url: str,
        settings: ProxySettings,
        binary: Path,
        tmp_path: Path,
        cache_dir: Path | None = None,
    ) -> ProxyHandle:
        """Launch a relaycache process and return a ready ProxyHandle."""
        if cache_dir is None:
            cache_dir = tmp_path / "cache"
            cache_dir.mkdir(exist_ok=True)

        base_env: dict[str, str] = {
            **os.environ,
            "CACHE_MAX_ENTRIES": str(settings.cache_max_entries),
            "MAX_CACHEABLE_SIZE": settings.max_cacheable_size,
            "ENTRY_TTL": settings.entry_ttl,
            "EVICTION_INTERVAL": settings.eviction_interval,
            "CACHE_DIR": str(cache_dir),
            "RUST_LOG": "relaycache=debug",
        }

        if settings.use_unix_socket:
            sock_path = tmp_path / "relaycache.sock"
            env = {**base_env, "UNIX_SOCKET": str(sock_path)}
            proc = subprocess.Popen(
                [str(binary), upstream_base_url],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + 5
            while not sock_path.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("unix socket did not appear")
                time.sleep(0.05)
            time.sleep(0.1)
            transport = httpx.HTTPTransport(uds=str(sock_path))
            client = httpx.Client(
                transport=transport,
                base_url="http://proxy/",
            )
        else:
            port = free_port()
            env = {**base_env, "BIND": f"127.0.0.1:{port}"}
            proc = subprocess.Popen(
                [str(binary), upstream_base_url],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            wait_for_port(port)
            client = httpx.Client(
                base_url=f"http://127.0.0.1:{port}",
            )

        return cls(
            client=client,
            _proc=proc,
            _upstream_base_url=upstream_base_url,
            _settings=settings,
            _binary=binary,
            _cache_dir=cache_dir,
            _tmp_path=tmp_path,
        )

    # -- Operations ----------------------------------------------------------

    def restart(self) -> None:
        """Terminate the current process and start a fresh one against the same cache_dir."""
        self._proc.terminate()
        self._proc.wait(timeout=5)
        self.client.close()
        fresh = ProxyHandle.start(
            upstream_base_url=self._upstream_base_url,
            settings=self._settings,
            binary=self._binary,
            tmp_path=self._tmp_path,
            cache_dir=self._cache_dir,
        )
        self.client = fresh.client
        self._proc = fresh._proc

    def terminate(self) -> None:
        self._proc.terminate()
        self._proc.wait(timeout=5)
        self.client.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_binary() -> Path:
    subprocess.run(["cargo", "build", "--release"], check=True)
    return PROXY_BIN


@pytest.fixture(scope="session")
def upstream_server_url() -> Generator[str, None, None]:
    port = free_port()
    config = uvicorn.Config(upstream_fastapi_app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    wait_for_port(port)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    t.join(timeout=5)


@pytest.fixture()
def upstream(upstream_server_url: str) -> Generator[Upstream, None, None]:
    """Fresh upstream session per test; destroyed on teardown."""
    up = Upstream.create(upstream_server_url)
    yield up
    up.close()


def _proxy_settings(request: pytest.FixtureRequest) -> ProxySettings:
    marker = request.node.get_closest_marker("proxy_settings")
    if marker and marker.args:
        return cast(ProxySettings, marker.args[0])
    return ProxySettings()


@pytest.fixture()
def proxy_handle(
    upstream: Upstream,
    proxy_binary: Path,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> Generator[ProxyHandle, None, None]:
    """
    ProxyHandle with process + client + restart capability.
    Configurable via @pytest.mark.proxy_settings(ProxySettings(...)).
    """
    handle = ProxyHandle.start(
        upstream_base_url=upstream.base_url,
        settings=_proxy_settings(request),
        binary=proxy_binary,
        tmp_path=tmp_path,
    )
    yield handle
    handle.terminate()


@pytest.fixture()
def client(proxy_handle: ProxyHandle) -> httpx.Client:
    """Plain httpx.Client pointed at the proxy.  Sufficient for most tests."""
    return proxy_handle.client


# ---------------------------------------------------------------------------
# TestBasicProxy
# ---------------------------------------------------------------------------


class TestBasicProxy:
    def test_first_request(self, client: httpx.Client, subtests: Any) -> None:
        r = client.get("/resource")
        with subtests.test("status 200"):
            assert r.status_code == 200
        with subtests.test("body correct"):
            assert r.content == RESOURCE_BODY
        with subtests.test("x-cache MISS"):
            assert r.headers.get("x-cache") == "MISS"
        with subtests.test("via header present"):
            assert "relaycache" in r.headers.get("via", "")
        with subtests.test("x-cache-key contains path"):
            assert "/resource" in r.headers.get("x-cache-key", "")

    def test_second_request_is_hit(self, client: httpx.Client, subtests: Any) -> None:
        client.get("/resource")  # prime
        r = client.get("/resource")
        with subtests.test("status 200"):
            assert r.status_code == 200
        with subtests.test("x-cache HIT"):
            assert r.headers.get("x-cache") == "HIT"

    def test_upstream_called_on_every_request(
        self, client: httpx.Client, upstream: Upstream
    ) -> None:
        """Proxy must revalidate with upstream on every request, even cache hits."""
        client.get("/resource")
        client.get("/resource")
        log = upstream.drain_log()
        assert len(log) == 2
        assert log[0].response_status == 200, "first request: upstream returned 200"
        assert log[1].response_status == 304, "second request: upstream confirmed not-modified"

    def test_v2_endpoint_returns_v2_body(self, client: httpx.Client) -> None:
        """Sanity check: /resource/v2 serves its own distinct body."""
        r = client.get("/resource/v2")
        assert r.content == b"new content"


# ---------------------------------------------------------------------------
# TestViaHeader
# ---------------------------------------------------------------------------


class TestViaHeader:
    def test_via_forwarded_to_upstream(self, client: httpx.Client, upstream: Upstream) -> None:
        client.get("/resource")
        log = upstream.drain_log()
        assert "relaycache" in log[-1].header("via")

    def test_via_appended_not_replaced(self, client: httpx.Client, upstream: Upstream) -> None:
        """relaycache must append to an existing Via, not replace it."""
        client.get("/resource", headers={"Via": "1.1 client-proxy"})
        log = upstream.drain_log()
        via = log[-1].header("via")
        assert "client-proxy" in via
        assert "relaycache" in via


# ---------------------------------------------------------------------------
# TestAuth
# ---------------------------------------------------------------------------


class TestAuth:
    def test_auth_forwarded(self, client: httpx.Client, upstream: Upstream) -> None:
        upstream.set_auth(required=True)
        r = client.get("/resource", headers={"Authorization": "Bearer secret"})
        assert r.status_code == 200

    def test_wrong_token_returns_401(self, client: httpx.Client, upstream: Upstream) -> None:
        upstream.set_auth(required=True)
        r = client.get("/resource", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401

    def test_missing_auth_returns_401(self, client: httpx.Client, upstream: Upstream) -> None:
        upstream.set_auth(required=True)
        assert client.get("/resource").status_code == 401

    def test_cached_body_not_served_after_auth_enabled(
        self, client: httpx.Client, upstream: Upstream
    ) -> None:
        """Core security guarantee: enabling auth takes effect immediately."""
        client.get("/resource")  # populate cache without auth
        upstream.set_auth(required=True)
        r = client.get("/resource")
        assert r.status_code == 401, "cached body must not bypass auth"
        assert r.headers.get("x-cache") != "HIT"


# ---------------------------------------------------------------------------
# TestConditional
# ---------------------------------------------------------------------------


class TestConditional:
    def test_client_inm_current_gets_304(self, client: httpx.Client) -> None:
        client.get("/resource")  # prime
        r = client.get("/resource", headers={"If-None-Match": RESOURCE_ETAG})
        assert r.status_code == 304

    def test_stale_client_inm_gets_cached_body(self, client: httpx.Client) -> None:
        """
        Client has a stale ETag (v0), proxy has cached v1.
        v0 doesn't match upstream's v1 so upstream returns 200; proxy must
        return 200 with the cached body rather than forwarding a 304.
        """
        client.get("/resource")  # prime with v1
        r = client.get("/resource", headers={"If-None-Match": '"v0"'})
        assert r.status_code == 200
        assert r.content == RESOURCE_BODY
        assert r.headers.get("x-cache") == "HIT"


# ---------------------------------------------------------------------------
# TestRange  (uses /blob/npython - large enough to stress range logic)
# ---------------------------------------------------------------------------


class TestRange:
    def test_range_from_cache(self, client: httpx.Client) -> None:
        client.get("/blob/npython")  # prime
        r = client.get("/blob/npython", headers={"Range": "bytes=0-1023"})
        assert r.status_code == 206
        assert r.content == BLOBS["npython"][:1024]
        assert r.headers.get("x-cache") == "HIT"

    def test_range_full_fetch_upgrade(self, client: httpx.Client) -> None:
        """First range request -> proxy fetches full body -> serves correct slice."""
        body = BLOBS["npython"]
        mid = len(body) // 2
        r = client.get("/blob/npython", headers={"Range": f"bytes={mid}-{mid + 1023}"})
        assert r.status_code == 206
        assert r.content == body[mid : mid + 1024]

    def test_range_content_range_header(self, client: httpx.Client) -> None:
        client.get("/blob/npython")  # prime
        total = len(BLOBS["npython"])
        r = client.get("/blob/npython", headers={"Range": "bytes=0-1023"})
        assert r.status_code == 206
        assert r.headers["content-range"] == f"bytes 0-1023/{total}"

    def test_range_unsatisfiable(self, client: httpx.Client) -> None:
        client.get("/blob/npython")  # prime
        body = BLOBS["npython"]
        r = client.get(
            "/blob/npython",
            headers={"Range": f"bytes={len(body) + 1}-{len(body) + 100}"},
        )
        assert r.status_code == 416

    def test_range_auth_enforced(self, client: httpx.Client, upstream: Upstream) -> None:
        client.get("/blob/npython")  # prime without auth
        upstream.set_auth(required=True)
        r = client.get("/blob/npython", headers={"Range": "bytes=0-1023"})
        assert r.status_code == 401, "auth must be enforced even on cached range requests"


# ---------------------------------------------------------------------------
# TestVary
# ---------------------------------------------------------------------------


class TestVary:
    def test_vary_accept_variants_cached_independently(
        self, client: httpx.Client, subtests: Any
    ) -> None:
        """
        Two requests with different Accept values must produce independent cache
        entries: each is a MISS on first fetch and a HIT on repeat, and each
        returns the body that corresponds to its own Accept value.
        """
        json_accept = "application/json"
        text_accept = "text/plain"

        with subtests.test("json variant first request is MISS"):
            r1 = client.get("/resource", headers={"Accept": json_accept})
            assert r1.status_code == 200
            assert r1.headers.get("x-cache") == "MISS"
            assert r1.content == json_accept.encode()

        with subtests.test("text variant first request is MISS"):
            r2 = client.get("/resource", headers={"Accept": text_accept})
            assert r2.status_code == 200
            assert r2.headers.get("x-cache") == "MISS"
            assert r2.content == text_accept.encode()

        with subtests.test("json variant second request is HIT"):
            r3 = client.get("/resource", headers={"Accept": json_accept})
            assert r3.status_code == 200
            assert r3.headers.get("x-cache") == "HIT"
            assert r3.content == json_accept.encode()

        with subtests.test("text variant second request is HIT"):
            r4 = client.get("/resource", headers={"Accept": text_accept})
            assert r4.status_code == 200
            assert r4.headers.get("x-cache") == "HIT"
            assert r4.content == text_accept.encode()

    def test_x_cache_key_includes_vary_dimension(self, client: httpx.Client) -> None:
        client.get("/resource", headers={"Accept": "application/json"})
        r = client.get("/resource", headers={"Accept": "application/json"})
        key = r.headers.get("x-cache-key", "")
        assert "accept=application/json" in key, f"Vary dimension missing: {key}"


# ---------------------------------------------------------------------------
# TestUnixSocket
# ---------------------------------------------------------------------------


@pytest.mark.proxy_settings(ProxySettings(use_unix_socket=True))
class TestUnixSocket:
    def test_basic_get(self, client: httpx.Client, subtests: Any) -> None:
        r = client.get("/resource")
        with subtests.test("status 200"):
            assert r.status_code == 200
        with subtests.test("body correct"):
            assert r.content == RESOURCE_BODY
        with subtests.test("via header"):
            assert "relaycache" in r.headers.get("via", "")
        with subtests.test("x-cache-key present"):
            assert r.headers.get("x-cache-key")

    def test_cache_hit(self, client: httpx.Client) -> None:
        client.get("/resource")
        r = client.get("/resource")
        assert r.headers.get("x-cache") == "HIT"


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_cache_survives_restart(self, proxy_handle: ProxyHandle) -> None:
        proxy_handle.client.get("/resource")  # prime
        assert proxy_handle.client.get("/resource").headers.get("x-cache") == "HIT"

        proxy_handle.restart()

        r = proxy_handle.client.get("/resource")
        assert r.headers.get("x-cache") == "HIT", "cache must survive proxy restart"


# ---------------------------------------------------------------------------
# TestEviction
# ---------------------------------------------------------------------------


@pytest.mark.proxy_settings(ProxySettings(entry_ttl="2s", eviction_interval="1s"))
class TestEviction:
    def test_entries_evicted_after_ttl(self, proxy_handle: ProxyHandle) -> None:
        proxy_handle.client.get("/resource")  # prime
        assert proxy_handle.client.get("/resource").headers.get("x-cache") == "HIT"
        time.sleep(5)  # wait for TTL + eviction cycle
        r = proxy_handle.client.get("/resource")
        assert r.headers.get("x-cache") == "MISS", "entry must be evicted after TTL"
