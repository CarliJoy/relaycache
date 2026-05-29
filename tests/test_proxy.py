"""
Integration tests for relaycache.

Prerequisites
-------------
    pip install pytest httpx

Run
---
    pytest tests/test_proxy.py -v

Architecture
------------
    pytest → relaycache (subprocess) → upstream (threading.HTTPServer in-process)
"""

from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest

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
# Controllable upstream server
# ---------------------------------------------------------------------------


class UpstreamState:
    def __init__(self) -> None:
        self.body: bytes = b"hello world"
        self.etag: str = '"v1"'
        self.last_modified: str = "Mon, 01 Jan 2024 00:00:00 GMT"
        self.auth_required: bool = False
        self.valid_token: str = "secret"
        self.request_log: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def reset(self) -> None:
        self.body = b"hello world"
        self.etag = '"v1"'
        self.last_modified = "Mon, 01 Jan 2024 00:00:00 GMT"
        self.auth_required = False
        self.valid_token = "secret"
        self.request_log = []

    def set_body(self, body: bytes, etag: str, last_modified: str = "") -> None:
        with self.lock:
            self.body = body
            self.etag = etag
            if last_modified:
                self.last_modified = last_modified

    def log(self, entry: dict[str, Any]) -> None:
        with self.lock:
            self.request_log.append(entry)

    def pop_log(self) -> list[dict[str, Any]]:
        with self.lock:
            log, self.request_log = self.request_log, []
        return log


class _StateHTTPServer(HTTPServer):
    """HTTPServer that carries UpstreamState so handlers can access it."""

    state: UpstreamState


class UpstreamHandler(BaseHTTPRequestHandler):
    server: _StateHTTPServer

    def log_message(self, *_: Any) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        state = self.server.state
        state.log({"path": self.path, "method": "GET", "headers": dict(self.headers)})

        if state.auth_required:
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {state.valid_token}":
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Bearer realm="test"')
                self.end_headers()
                return

        body = state.body
        etag = state.etag
        lm = state.last_modified

        # ETag conditional
        inm = self.headers.get("If-None-Match", "")
        if inm:
            tokens = [t.strip().lstrip("W/").strip('"') for t in inm.split(",")]
            our_tag = etag.lstrip("W/").strip('"')
            if our_tag in tokens:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.send_header("Last-Modified", lm)
                self.end_headers()
                return

        # Date conditional (RFC 7232 §3.3: ignore IMS when INM is present)
        ims = self.headers.get("If-Modified-Since", "")
        if ims and ims == lm and not inm:
            self.send_response(304)
            self.send_header("Last-Modified", lm)
            self.end_headers()
            return

        # Range
        range_hdr = self.headers.get("Range", "")
        if range_hdr.startswith("bytes="):
            spec = range_hdr[len("bytes=") :]
            start_s, _, end_s = spec.partition("-")
            start = int(start_s) if start_s else max(0, len(body) - int(end_s))
            end = (int(end_s) + 1) if end_s else len(body)
            end = min(end, len(body))
            chunk = body[start:end]
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{len(body)}")
            self.send_header("ETag", etag)
            self.send_header("Last-Modified", lm)
            self.end_headers()
            self.wfile.write(chunk)
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", etag)
        self.send_header("Last-Modified", lm)
        self.send_header("Vary", "Accept")
        self.end_headers()
        self.wfile.write(body)


class UpstreamServer:
    """Running upstream HTTP server together with its mutable state."""

    def __init__(self) -> None:
        self.state = UpstreamState()
        port = free_port()
        self._server = _StateHTTPServer(("127.0.0.1", port), UpstreamHandler)
        self._server.state = self.state
        self.url = f"http://127.0.0.1:{port}"

    def start(self) -> None:
        t = threading.Thread(target=self._server.serve_forever, daemon=True)
        t.start()

    def shutdown(self) -> None:
        self._server.shutdown()


# ---------------------------------------------------------------------------
# Proxy wrapper
# ---------------------------------------------------------------------------


class Proxy:
    """Thin client wrapper around a running relaycache process."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url

    def get(self, path: str = "/resource", **kwargs: Any) -> httpx.Response:
        return httpx.get(f"{self._base_url}{path}", **kwargs)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_binary() -> Path:
    subprocess.run(["cargo", "build", "--release"], check=True)
    return Path(__file__).parent.parent / "target" / "release" / "relaycache"


@pytest.fixture(scope="session")
def upstream_server() -> Generator[UpstreamServer, None, None]:
    server = UpstreamServer()
    server.start()
    yield server
    server.shutdown()


@pytest.fixture()
def upstream(upstream_server: UpstreamServer) -> UpstreamServer:
    upstream_server.state.reset()
    return upstream_server


def _start_proxy(
    upstream_url: str,
    extra_env: dict[str, str],
    tmp_path: Path,
    binary: Path,
    cache_dir: Path | None = None,
) -> tuple[str, subprocess.Popen[bytes]]:
    port = free_port()
    if cache_dir is None:
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
    env: dict[str, str] = {
        **os.environ,
        "BIND": f"127.0.0.1:{port}",
        "CACHE_MAX_ENTRIES": "256",
        "MAX_CACHEABLE_SIZE": "10MiB",
        "ENTRY_TTL": "1h",
        "EVICTION_INTERVAL": "2h",
        "CACHE_DIR": str(cache_dir),
        "RUST_LOG": "relaycache=debug",
        **extra_env,
    }
    proc = subprocess.Popen(
        [str(binary), upstream_url],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    wait_for_port(port)
    return f"http://127.0.0.1:{port}", proc


@pytest.fixture()
def proxy(
    upstream: UpstreamServer, proxy_binary: Path, tmp_path: Path
) -> Generator[Proxy, None, None]:
    url, proc = _start_proxy(upstream.url, {}, tmp_path, proxy_binary)
    yield Proxy(url)
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture()
def proxy_unix(
    upstream: UpstreamServer, proxy_binary: Path, tmp_path: Path
) -> Generator[httpx.Client, None, None]:
    sock_path = tmp_path / "relaycache.sock"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    env: dict[str, str] = {
        **os.environ,
        "UNIX_SOCKET": str(sock_path),
        "CACHE_MAX_ENTRIES": "256",
        "MAX_CACHEABLE_SIZE": "10MiB",
        "ENTRY_TTL": "1h",
        "EVICTION_INTERVAL": "2h",
        "CACHE_DIR": str(cache_dir),
        "RUST_LOG": "relaycache=debug",
    }
    proc = subprocess.Popen(
        [str(proxy_binary), upstream.url],
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
    client = httpx.Client(transport=transport, base_url="http://proxy")
    yield client
    client.close()
    proc.terminate()
    proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Basic proxying
# ---------------------------------------------------------------------------


class TestBasicProxy:
    def test_200_forwarded(self, proxy: Proxy) -> None:
        r = proxy.get()
        assert r.status_code == 200
        assert r.content == b"hello world"

    def test_via_header_on_response(self, proxy: Proxy) -> None:
        r = proxy.get()
        assert "relaycache" in r.headers.get("via", ""), f"via={r.headers.get('via')}"

    def test_x_cache_miss_first_request(self, proxy: Proxy) -> None:
        r = proxy.get()
        assert r.headers.get("x-cache") == "MISS"

    def test_x_cache_hit_second_request(self, proxy: Proxy) -> None:
        proxy.get()
        r = proxy.get()
        assert r.status_code == 200
        assert r.headers.get("x-cache") == "HIT"

    def test_x_cache_key_present(self, proxy: Proxy) -> None:
        r = proxy.get()
        key = r.headers.get("x-cache-key", "")
        assert "/resource" in key, f"x-cache-key={key}"

    def test_upstream_always_receives_request(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        proxy.get()
        proxy.get()
        assert len(upstream.state.pop_log()) == 2, "upstream must be called on every request"

    def test_body_change_invalidates_cache(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        proxy.get()
        upstream.state.set_body(b"new content", '"v2"')
        r = proxy.get()
        assert r.content == b"new content"


# ---------------------------------------------------------------------------
# Via header
# ---------------------------------------------------------------------------


class TestViaHeader:
    def test_via_forwarded_to_upstream(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        proxy.get()
        log = upstream.state.pop_log()
        via = log[-1]["headers"].get("Via", log[-1]["headers"].get("via", ""))
        assert "relaycache" in via, f"via={via}"

    def test_via_appended_not_replaced(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        """If the client sends a Via header, relaycache should append, not replace."""
        proxy.get(headers={"Via": "1.1 client-proxy"})
        log = upstream.state.pop_log()
        via = log[-1]["headers"].get("Via", log[-1]["headers"].get("via", ""))
        assert "client-proxy" in via, f"upstream via={via}"
        assert "relaycache" in via, f"upstream via={via}"


# ---------------------------------------------------------------------------
# Auth forwarding
# ---------------------------------------------------------------------------


class TestAuth:
    def test_auth_forwarded(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        upstream.state.auth_required = True
        r = proxy.get(headers={"Authorization": "Bearer secret"})
        assert r.status_code == 200

    def test_wrong_auth_401(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        upstream.state.auth_required = True
        r = proxy.get(headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401

    def test_no_auth_401(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        upstream.state.auth_required = True
        r = proxy.get()
        assert r.status_code == 401

    def test_cached_body_not_served_to_unauthorized(
        self, proxy: Proxy, upstream: UpstreamServer
    ) -> None:
        """Core security guarantee: revoked access takes effect immediately."""
        proxy.get()  # populate cache
        upstream.state.auth_required = True
        r = proxy.get()  # no auth header
        assert r.status_code == 401, "cached body must not be served to unauthorized client"
        assert r.headers.get("x-cache") != "HIT", "x-cache must not be HIT on auth failure"


# ---------------------------------------------------------------------------
# Conditional requests
# ---------------------------------------------------------------------------


class TestConditional:
    def test_client_inm_current_gets_304(self, proxy: Proxy) -> None:
        proxy.get()  # prime
        r = proxy.get(headers={"If-None-Match": '"v1"'})
        assert r.status_code == 304

    def test_stale_client_inm_gets_cached_body(self, proxy: Proxy) -> None:
        """
        Client has stale ETag ("v0"), proxy has cached "v1".
        Upstream says 304 (for "v1"); client's "v0" didn't match →
        proxy must return 200 with cached body, not 304.
        """
        proxy.get()  # prime with v1
        r = proxy.get(headers={"If-None-Match": '"v0"'})
        assert r.status_code == 200
        assert r.content == b"hello world"
        assert r.headers.get("x-cache") == "HIT"


# ---------------------------------------------------------------------------
# Range requests
# ---------------------------------------------------------------------------


class TestRange:
    def test_range_from_cache(self, proxy: Proxy) -> None:
        proxy.get()  # prime
        r = proxy.get(headers={"Range": "bytes=0-4"})
        assert r.status_code == 206
        assert r.content == b"hello"
        assert r.headers.get("x-cache") == "HIT"

    def test_range_full_fetch_upgrade(self, proxy: Proxy) -> None:
        """First range request → full fetch → serve correct slice."""
        r = proxy.get(headers={"Range": "bytes=6-10"})
        assert r.status_code == 206
        assert r.content == b"world"

    def test_range_content_range_header(self, proxy: Proxy) -> None:
        proxy.get()  # prime
        r = proxy.get(headers={"Range": "bytes=0-4"})
        assert r.status_code == 206
        assert r.headers["content-range"] == "bytes 0-4/11"

    def test_range_unsatisfiable(self, proxy: Proxy) -> None:
        proxy.get()  # prime
        r = proxy.get(headers={"Range": "bytes=9999-99999"})
        assert r.status_code == 416

    def test_range_auth_enforced(self, proxy: Proxy, upstream: UpstreamServer) -> None:
        proxy.get()  # prime without auth
        upstream.state.auth_required = True
        r = proxy.get(headers={"Range": "bytes=0-4"})
        assert r.status_code == 401, "auth must be enforced even when serving cached range"


# ---------------------------------------------------------------------------
# Vary
# ---------------------------------------------------------------------------


class TestVary:
    def test_vary_accept_two_requests_reach_upstream(
        self, proxy: Proxy, upstream: UpstreamServer
    ) -> None:
        """Different Accept values must not share a cache entry."""
        upstream.state.pop_log()
        proxy.get(headers={"Accept": "application/json"})
        proxy.get(headers={"Accept": "text/plain"})
        log = upstream.state.pop_log()
        assert len(log) == 2, "both Vary variants must reach the upstream"

    def test_x_cache_key_includes_vary_dimension(self, proxy: Proxy) -> None:
        proxy.get(headers={"Accept": "application/json"})
        r = proxy.get(headers={"Accept": "application/json"})
        key = r.headers.get("x-cache-key", "")
        assert "accept=application/json" in key, f"Vary dimension missing from key: {key}"


# ---------------------------------------------------------------------------
# Unix socket
# ---------------------------------------------------------------------------


class TestUnixSocket:
    def test_get_via_unix_socket(self, proxy_unix: httpx.Client) -> None:
        r = proxy_unix.get("/resource")
        assert r.status_code == 200
        assert r.content == b"hello world"

    def test_via_header_unix_socket(self, proxy_unix: httpx.Client) -> None:
        r = proxy_unix.get("/resource")
        assert "relaycache" in r.headers.get("via", "")

    def test_cache_hit_unix_socket(self, proxy_unix: httpx.Client) -> None:
        proxy_unix.get("/resource")
        r = proxy_unix.get("/resource")
        assert r.headers.get("x-cache") == "HIT"

    def test_x_cache_key_unix_socket(self, proxy_unix: httpx.Client) -> None:
        r = proxy_unix.get("/resource")
        assert r.headers.get("x-cache-key"), "x-cache-key must be present"


# ---------------------------------------------------------------------------
# Persistence across restart
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_cache_survives_restart(
        self, upstream: UpstreamServer, proxy_binary: Path, tmp_path: Path
    ) -> None:
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        url, proc = _start_proxy(upstream.url, {}, tmp_path, proxy_binary, cache_dir=cache_dir)
        httpx.get(f"{url}/resource")  # prime
        assert httpx.get(f"{url}/resource").headers.get("x-cache") == "HIT"
        proc.terminate()
        proc.wait(timeout=5)

        url, proc = _start_proxy(upstream.url, {}, tmp_path, proxy_binary, cache_dir=cache_dir)
        try:
            r = httpx.get(f"{url}/resource")
            assert r.headers.get("x-cache") == "HIT", "cache must survive proxy restart"
        finally:
            proc.terminate()
            proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# TTL eviction
# ---------------------------------------------------------------------------


class TestEviction:
    def test_entries_evicted_after_ttl(
        self, upstream: UpstreamServer, proxy_binary: Path, tmp_path: Path
    ) -> None:
        url, proc = _start_proxy(
            upstream.url,
            {"ENTRY_TTL": "2s", "EVICTION_INTERVAL": "1s"},
            tmp_path,
            proxy_binary,
        )
        try:
            httpx.get(f"{url}/resource")  # prime
            assert httpx.get(f"{url}/resource").headers.get("x-cache") == "HIT"
            time.sleep(5)  # wait for TTL expiry + eviction cycle
            r = httpx.get(f"{url}/resource")
            assert r.headers.get("x-cache") == "MISS", "entry must be evicted after TTL"
        finally:
            proc.terminate()
            proc.wait(timeout=5)
