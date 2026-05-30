"""
Tests for upstream_app.py (the fake FastAPI upstream) and the Upstream wrapper.

These run entirely in-process - no relaycache binary required.

Architecture
------------
    pytest -> FastAPI (via httpx.AsyncClient / TestClient) and Upstream wrapper

Sections
--------
  TestSessionLifecycle    PUT / PATCH / DELETE / 404 on unknown session
  TestLog                 drain semantics, header tuple format
  TestConditional         RFC 7232: INM, IMS, precedence
  TestResource            /resource, /resource/v2
  TestBlob                /blob/npython streaming + range + unknown name
  TestUpstreamWrapper     Upstream class: create, set_auth, set_blob_identity,
                          reset_settings, drain_log, close
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn
from upstream import Upstream
from upstream_app import (
    BLOBS,
    RESOURCE_BODY,
    RESOURCE_ETAG,
    RESOURCE_LM,
    V2_BODY,
    V2_ETAG,
    LogEntry,
    SessionSettings,
    app,
)

# ---------------------------------------------------------------------------
# Test server fixture (session-scoped, single process)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    import socket
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"port {port} did not open in {timeout}s")


@pytest.fixture(scope="session")
def server_url() -> Generator[str, None, None]:
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    _wait_for_port(port)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    t.join(timeout=5)


@pytest.fixture()
def client(server_url: str) -> Generator[httpx.Client, None, None]:
    with httpx.Client(base_url=server_url) as c:
        yield c


@pytest.fixture()
def session(server_url: str) -> Generator[str, None, None]:
    """Create a fresh session and yield its string UUID.  Deleted on teardown."""
    sid = str(uuid4())
    resp = httpx.put(f"{server_url}/tests/{sid}")
    resp.raise_for_status()
    yield sid
    httpx.delete(f"{server_url}/tests/{sid}")


@pytest.fixture()
def upstream(server_url: str) -> Generator[Upstream, None, None]:
    """Upstream wrapper fixture - session created and destroyed automatically."""
    up = Upstream.create(server_url)
    yield up
    up.close()


# ---------------------------------------------------------------------------
# TestSessionLifecycle
# ---------------------------------------------------------------------------


class TestSessionLifecycle:
    def test_put_creates_session(self, client: httpx.Client) -> None:
        sid = str(uuid4())
        r = client.put(f"/tests/{sid}")
        assert r.status_code == 201
        assert r.json()["session_id"] == sid
        client.delete(f"/tests/{sid}")  # cleanup

    def test_put_recreates_session(self, client: httpx.Client, session: str) -> None:
        # Second PUT on same ID must succeed (idempotent create)
        r = client.put(f"/tests/{session}")
        assert r.status_code == 201

    def test_delete_removes_session(self, client: httpx.Client) -> None:
        sid = str(uuid4())
        client.put(f"/tests/{sid}").raise_for_status()
        r = client.delete(f"/tests/{sid}")
        assert r.status_code == 204
        # Subsequent resource access must return 404
        r2 = client.get(f"/tests/{sid}/resource")
        assert r2.status_code == 404

    def test_unknown_session_returns_404(self, client: httpx.Client) -> None:
        missing = str(uuid4())
        assert client.get(f"/tests/{missing}/resource").status_code == 404
        assert client.get(f"/logs/{missing}").status_code == 404
        assert (
            client.patch(
                f"/tests/{missing}",
                content=SessionSettings().model_dump_json(),
                headers={"Content-Type": "application/json"},
            ).status_code
            == 404
        )

    def test_patch_updates_settings(self, client: httpx.Client, session: str) -> None:
        new_settings = SessionSettings(auth_required=True, valid_token="tok123")
        r = client.patch(
            f"/tests/{session}",
            content=new_settings.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 200
        returned = SessionSettings.model_validate(r.json())
        assert returned.auth_required is True
        assert returned.valid_token == "tok123"


# ---------------------------------------------------------------------------
# TestLog
# ---------------------------------------------------------------------------


class TestLog:
    def test_log_records_request(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource")
        log = client.get(f"/logs/{session}").json()
        assert len(log) == 1
        entry = LogEntry.model_validate(log[0])
        assert entry.method == "GET"
        assert f"/tests/{session}/resource" in entry.path

    def test_log_is_drained_on_get(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource")
        client.get(f"/logs/{session}")  # drain
        log2 = client.get(f"/logs/{session}").json()
        assert log2 == []

    def test_log_accumulates_multiple_requests(self, client: httpx.Client, session: str) -> None:
        for _ in range(3):
            client.get(f"/tests/{session}/resource")
        log = client.get(f"/logs/{session}").json()
        assert len(log) == 3

    def test_headers_are_list_of_pairs(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource", headers={"X-Custom": "hello"})
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        # Each element must be a 2-element list
        for pair in entry.headers:
            assert len(pair) == 2, f"header pair has {len(pair)} elements: {pair}"
        assert entry.header("x-custom") == "hello"

    def test_header_lookup_is_case_insensitive(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource", headers={"X-Probe": "value"})
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        assert entry.header("X-Probe") == "value"
        assert entry.header("x-probe") == "value"
        assert entry.header("X-PROBE") == "value"

    def test_missing_header_returns_empty_string(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource")
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        assert entry.header("x-does-not-exist") == ""

    def test_response_status_stamped_on_200(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource")
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        assert entry.response_status == 200

    def test_response_status_stamped_on_304(self, client: httpx.Client, session: str) -> None:
        client.get(f"/tests/{session}/resource", headers={"If-None-Match": RESOURCE_ETAG})
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        assert entry.response_status == 304

    def test_response_status_stamped_on_401(self, client: httpx.Client, session: str) -> None:
        client.patch(
            f"/tests/{session}",
            content=SessionSettings(auth_required=True).model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        client.get(f"/tests/{session}/resource")  # no auth -> 401
        log = client.get(f"/logs/{session}").json()
        entry = LogEntry.model_validate(log[0])
        assert entry.response_status == 401

    def test_sessions_do_not_share_logs(self, server_url: str) -> None:
        up_a = Upstream.create(server_url)
        up_b = Upstream.create(server_url)
        try:
            httpx.get(f"{up_a.base_url}/resource")
            # Only session A should have a log entry
            assert len(up_a.drain_log()) == 1
            assert len(up_b.drain_log()) == 0
        finally:
            up_a.close()
            up_b.close()


# ---------------------------------------------------------------------------
# TestConditional  (RFC 7232)
# ---------------------------------------------------------------------------


class TestConditional:
    def test_inm_match_returns_304(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/resource",
            headers={"If-None-Match": RESOURCE_ETAG},
        )
        assert r.status_code == 304

    def test_inm_mismatch_returns_200(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/resource",
            headers={"If-None-Match": '"stale-tag"'},
        )
        assert r.status_code == 200

    def test_inm_weak_tag_matches(self, client: httpx.Client, session: str) -> None:
        weak = f"W/{RESOURCE_ETAG}"
        r = client.get(f"/tests/{session}/resource", headers={"If-None-Match": weak})
        assert r.status_code == 304

    def test_inm_star_matches(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource", headers={"If-None-Match": "*"})
        assert r.status_code == 304

    def test_ims_match_returns_304(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/resource",
            headers={"If-Modified-Since": RESOURCE_LM},
        )
        assert r.status_code == 304

    def test_ims_older_date_returns_200(self, client: httpx.Client, session: str) -> None:
        older = "Mon, 01 Jan 2001 00:00:00 GMT"
        r = client.get(
            f"/tests/{session}/resource",
            headers={"If-Modified-Since": older},
        )
        assert r.status_code == 200

    def test_inm_takes_precedence_over_ims(self, client: httpx.Client, session: str) -> None:
        """When INM is present and doesn't match, IMS must be ignored (RFC 7232 §3.3)."""
        r = client.get(
            f"/tests/{session}/resource",
            headers={
                "If-None-Match": '"stale-tag"',  # doesn't match -> would normally 200
                "If-Modified-Since": RESOURCE_LM,  # would cause 304 if evaluated
            },
        )
        # INM wins: stale tag -> no match -> 200 (IMS ignored)
        assert r.status_code == 200

    def test_304_carries_etag_header(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/resource",
            headers={"If-None-Match": RESOURCE_ETAG},
        )
        assert r.status_code == 304
        assert r.headers.get("etag") == RESOURCE_ETAG


# ---------------------------------------------------------------------------
# TestResource
# ---------------------------------------------------------------------------


class TestResource:
    def test_200_body_and_etag(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource")
        assert r.status_code == 200
        assert r.content == RESOURCE_BODY
        assert r.headers["etag"] == RESOURCE_ETAG

    def test_vary_accept_header_present(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource")
        assert "accept" in r.headers.get("vary", "").lower()

    def test_auth_required_blocks_without_token(self, client: httpx.Client, session: str) -> None:
        client.patch(
            f"/tests/{session}",
            content=SessionSettings(auth_required=True).model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        r = client.get(f"/tests/{session}/resource")
        assert r.status_code == 401

    def test_auth_passes_with_correct_token(self, client: httpx.Client, session: str) -> None:
        client.patch(
            f"/tests/{session}",
            content=SessionSettings(auth_required=True, valid_token="tok").model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        r = client.get(f"/tests/{session}/resource", headers={"Authorization": "Bearer tok"})
        assert r.status_code == 200

    def test_v2_different_body_and_etag(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource/v2")
        assert r.status_code == 200
        assert r.content == V2_BODY
        assert r.headers["etag"] == V2_ETAG

    def test_range_request(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource", headers={"Range": "bytes=0-4"})
        assert r.status_code == 206
        assert r.content == b"hello"
        assert r.headers["content-range"] == "bytes 0-4/11"

    def test_range_unsatisfiable(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/resource", headers={"Range": "bytes=9999-99999"})
        assert r.status_code == 416


# ---------------------------------------------------------------------------
# TestBlob
# ---------------------------------------------------------------------------


class TestBlob:
    def test_npython_returns_200(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/blob/npython")
        assert r.status_code == 200
        assert len(r.content) == len(BLOBS["npython"])

    def test_npython_content_correct(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/blob/npython")
        python_bin = Path(sys.executable).read_bytes()
        assert r.content == python_bin * 10

    def test_npython_etag_present(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/blob/npython")
        assert r.headers.get("etag") == '"blob-v1"'

    def test_unknown_blob_returns_404(self, client: httpx.Client, session: str) -> None:
        r = client.get(f"/tests/{session}/blob/doesnotexist")
        assert r.status_code == 404

    def test_blob_range_request(self, client: httpx.Client, session: str) -> None:
        body = BLOBS["npython"]
        end_byte = 1024 * 1024 - 1  # first MiB
        r = client.get(
            f"/tests/{session}/blob/npython",
            headers={"Range": f"bytes=0-{end_byte}"},
        )
        assert r.status_code == 206
        assert len(r.content) == 1024 * 1024
        assert r.content == body[: 1024 * 1024]
        assert r.headers["content-range"] == f"bytes 0-{end_byte}/{len(body)}"

    def test_blob_range_unsatisfiable(self, client: httpx.Client, session: str) -> None:
        body = BLOBS["npython"]
        r = client.get(
            f"/tests/{session}/blob/npython",
            headers={"Range": f"bytes={len(body) + 1}-{len(body) + 100}"},
        )
        assert r.status_code == 416

    def test_blob_304_on_matching_etag(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/blob/npython",
            headers={"If-None-Match": '"blob-v1"'},
        )
        assert r.status_code == 304

    def test_blob_200_after_etag_change(self, client: httpx.Client, session: str) -> None:
        """Changing blob_etag via PATCH causes the upstream to return 200 again."""
        client.patch(
            f"/tests/{session}",
            content=SessionSettings(blob_etag='"blob-v2"').model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        r = client.get(
            f"/tests/{session}/blob/npython",
            headers={"If-None-Match": '"blob-v1"'},  # old tag
        )
        assert r.status_code == 200
        assert r.headers["etag"] == '"blob-v2"'

    def test_blob_ims_304(self, client: httpx.Client, session: str) -> None:
        r = client.get(
            f"/tests/{session}/blob/npython",
            headers={"If-Modified-Since": "Mon, 01 Jan 2024 00:00:00 GMT"},
        )
        assert r.status_code == 304


# ---------------------------------------------------------------------------
# TestUpstreamWrapper
# ---------------------------------------------------------------------------


class TestUpstreamWrapper:
    def test_create_returns_upstream(self, server_url: str) -> None:
        up = Upstream.create(server_url)
        assert up.base_url.endswith(str(up.session_id))
        up.close()

    def test_base_url_contains_session_id(self, upstream: Upstream) -> None:
        assert str(upstream.session_id) in upstream.base_url

    def test_drain_log_returns_entries(self, server_url: str, upstream: Upstream) -> None:
        httpx.get(f"{upstream.base_url}/resource")
        log = upstream.drain_log()
        assert len(log) == 1
        assert isinstance(log[0], LogEntry)

    def test_drain_log_clears_log(self, upstream: Upstream) -> None:
        httpx.get(f"{upstream.base_url}/resource")
        upstream.drain_log()
        assert upstream.drain_log() == []

    def test_set_auth_blocks_unauthenticated(self, upstream: Upstream) -> None:
        upstream.set_auth(required=True)
        r = httpx.get(f"{upstream.base_url}/resource")
        assert r.status_code == 401

    def test_set_auth_allows_correct_token(self, upstream: Upstream) -> None:
        upstream.set_auth(required=True, token="mytoken")
        r = httpx.get(
            f"{upstream.base_url}/resource",
            headers={"Authorization": "Bearer mytoken"},
        )
        assert r.status_code == 200

    def test_reset_settings_clears_auth(self, upstream: Upstream) -> None:
        upstream.set_auth(required=True)
        upstream.reset_settings()
        r = httpx.get(f"{upstream.base_url}/resource")
        assert r.status_code == 200

    def test_set_blob_identity_changes_etag(self, upstream: Upstream) -> None:
        upstream.set_blob_identity(
            etag='"custom-etag"',
            last_modified="Fri, 01 Jan 2099 00:00:00 GMT",
        )
        r = httpx.get(f"{upstream.base_url}/blob/npython")
        assert r.headers["etag"] == '"custom-etag"'

    def test_set_blob_identity_affects_304(self, upstream: Upstream) -> None:
        upstream.set_blob_identity(
            etag='"new-etag"',
            last_modified="Fri, 01 Jan 2099 00:00:00 GMT",
        )
        r = httpx.get(
            f"{upstream.base_url}/blob/npython",
            headers={"If-None-Match": '"new-etag"'},
        )
        assert r.status_code == 304

    def test_close_deletes_session(self, server_url: str) -> None:
        up = Upstream.create(server_url)
        sid = up.session_id
        up.close()
        r = httpx.get(f"{server_url}/tests/{sid}/resource")
        assert r.status_code == 404

    def test_update_settings_returns_confirmed_settings(self, upstream: Upstream) -> None:
        new = SessionSettings(auth_required=True, valid_token="x", blob_etag='"e"')
        confirmed = upstream.update_settings(new)
        assert confirmed.auth_required is True
        assert confirmed.blob_etag == '"e"'
