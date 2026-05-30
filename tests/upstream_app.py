"""
Fake upstream HTTP server for relaycache integration tests.

Session lifecycle
-----------------
Each test gets its own isolated session so parallel runs never cross-contaminate
logs or configuration.  Single uvicorn worker = no concurrent access, no locks.

    PUT    /tests/{session_id}          create (or re-create) a session
    PATCH  /tests/{session_id}          partially update session settings
    DELETE /tests/{session_id}          destroy session

    GET    /logs/{session_id}           return (and clear) the request log

Resource endpoints  (all record into the owning session's log)
--------------------------------------------------------------
Proxy clients use  /tests/{session_id}  as their base URL, so they hit:

    GET  /tests/{session_id}/resource           basic resource (ETag v1, Vary: Accept)
    GET  /tests/{session_id}/resource/v2        different body + ETag (cache-invalidation)
    GET  /tests/{session_id}/blob/{what}        large binary body (~80 MB)

    Supported {what} values:
        npython   - Python interpreter binary x10

Blob settings (per session, via PATCH)
---------------------------------------
    blob_etag          strong ETag string, default '"blob-v1"'
    blob_last_modified RFC 5322 date string, default "Mon, 01 Jan 2024 00:00:00 GMT"

Conditional request handling  (RFC 7232)
----------------------------------------
Implemented in conditional_or_serve():
  1. If-None-Match takes precedence over If-Modified-Since (section 6 evaluation order).
  2. Weak-tag comparison per section 2.3 (W/ prefix stripped, quotes stripped).
  3. If-Modified-Since ignored when If-None-Match is present (section 3.3).

Log format
----------
Every recorded entry carries:

    {
        "path":    "/tests/<id>/resource",
        "method":  "GET",
        "headers": [["header-name", "value"], ...]
    }

Headers are a list of [name, value] pairs (preserves insertion order and
duplicate names).  Import LogEntry to parse them with .header(name).
"""

from __future__ import annotations

import sys
import types
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from email.utils import parsedate
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Shared models  (import in tests for type-safe log access)
# ---------------------------------------------------------------------------


class LogEntry(BaseModel):
    path: str
    method: str
    # Wire format: [[name, value], ...]  - JSON has no tuple type, so list[list[str]]
    headers: list[list[str]]
    # Filled in by middleware after the response is produced; None if not yet set
    response_status: int | None = None

    def header(self, name: str) -> str:
        """Return the first value for *name* (case-insensitive), or ''."""
        lower = name.lower()
        for k, v in self.headers:
            if k.lower() == lower:
                return v
        return ""


class SessionSettings(BaseModel):
    """Full session settings - all fields required, used as the canonical stored state."""

    auth_required: bool = False
    valid_token: str = "secret"
    # Controls ETag and Last-Modified advertised for /blob/* endpoints
    blob_etag: str = '"blob-v1"'
    blob_last_modified: str = "Mon, 01 Jan 2024 00:00:00 GMT"


class SessionSettingsPatch(BaseModel):
    """Partial update payload - only submitted (non-None) fields are applied."""

    auth_required: bool | None = None
    valid_token: str | None = None
    blob_etag: str | None = None
    blob_last_modified: str | None = None


# ---------------------------------------------------------------------------
# Session  (no private underscore - it's part of the public contract)
# ---------------------------------------------------------------------------


@dataclass
class Session:
    id: UUID
    settings: SessionSettings = field(default_factory=SessionSettings)
    log: list[LogEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Immutable blob registry  (MappingProxyType - no accidental mutation)
# ---------------------------------------------------------------------------

_PYTHON_BIN: bytes = Path(sys.executable).read_bytes()

BLOBS: Mapping[str, bytes] = types.MappingProxyType(
    {
        "npython": _PYTHON_BIN * 10,  # ~80 MB
    }
)

# ---------------------------------------------------------------------------
# Immutable resource constants
# ---------------------------------------------------------------------------

RESOURCE_BODY: bytes = b"hello world"
RESOURCE_ETAG: str = '"v1"'
RESOURCE_LM: str = "Mon, 01 Jan 2024 00:00:00 GMT"

V2_BODY: bytes = b"new content"
V2_ETAG: str = '"v2"'
V2_LM: str = "Tue, 02 Jan 2024 00:00:00 GMT"

# ---------------------------------------------------------------------------
# App + lifespan  (sessions live on app.state, not as a module global)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    app.state.sessions: dict[UUID, Session] = {}
    yield
    app.state.sessions.clear()


app = FastAPI(title="relaycache fake upstream", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Middleware: stamp response_status onto the last log entry for the session
# ---------------------------------------------------------------------------
#
# Regular dependencies run before the response is produced so they cannot see
# the status code.  A Starlette middleware wraps the full cycle and can patch
# the log entry after call_next() returns.
#
# Only resource routes (paths matching /tests/{uuid}/...) are stamped;
# management routes (/logs/*, PUT/PATCH/DELETE /tests/*) are skipped.

import re as _re

_RESOURCE_PATH_RE = _re.compile(
    r"^/tests/[0-9a-f-]{36}/(?!$)",  # /tests/{uuid}/<something>
    _re.IGNORECASE,
)


@app.middleware("http")
async def stamp_response_status(request: Request, call_next: Any) -> Response:
    response = await call_next(request)
    if _RESOURCE_PATH_RE.match(request.url.path):
        # Extract session_id from path and stamp the last log entry
        try:
            sid = UUID(request.url.path.split("/")[2])
            sessions: dict[UUID, Session] = request.app.state.sessions
            session = sessions.get(sid)
            if session and session.log:
                session.log[-1] = session.log[-1].model_copy(
                    update={"response_status": response.status_code}
                )
        except (ValueError, IndexError):
            pass
    return response


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def get_sessions(request: Request) -> dict[UUID, Session]:
    """Return the live sessions dict from app state."""
    return request.app.state.sessions  # type: ignore[no-any-return]


SessionsDict = Annotated[dict[UUID, Session], Depends(get_sessions)]


def get_session(session_id: UUID, sessions: SessionsDict) -> Session:
    """Resolve session_id to a Session or raise 404."""
    session = sessions.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"session {session_id} not found")
    return session


def record_and_check_auth(
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> Session:
    """
    Append the incoming request to the session log, then enforce auth if required.

    Always records first - even rejected requests appear in the log so tests can
    assert that the upstream received and rejected the request.
    Raises HTTP 401 if auth is required and the token is wrong or absent.
    """
    session.log.append(
        LogEntry(
            path=request.url.path,
            method=request.method,
            headers=[[k.decode(), v.decode()] for k, v in request.headers.raw],
        )
    )
    if session.settings.auth_required:
        auth = request.headers.get("authorization", "")
        if auth != f"Bearer {session.settings.valid_token}":
            raise HTTPException(
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer realm="test"'},
            )
    return session


# Annotated alias used by all resource routes
SessionDep = Annotated[Session, Depends(record_and_check_auth)]

# ---------------------------------------------------------------------------
# RFC 7232 helpers
# ---------------------------------------------------------------------------


def _etag_matches(client_inm: str, server_etag: str) -> bool:
    """Weak comparison: strip W/ and quotes before comparing (RFC 7232 §2.3)."""

    def _bare(t: str) -> str:
        return t.strip().lstrip("W/").strip('"')

    server_bare = _bare(server_etag)
    for token in client_inm.split(","):
        if token.strip() == "*" or _bare(token) == server_bare:
            return True
    return False


def conditional_or_serve(
    request: Request,
    body: bytes,
    etag: str,
    last_modified: str,
    extra_headers: dict[str, str] | None = None,
) -> Response:
    """
    Apply RFC 7232 conditional-GET logic, then serve the body.

    Decision order:
      1. If-None-Match present and matches  -> 304
      2. If-None-Match present, no match    -> skip IMS, fall through to body
      3. If-Modified-Since present and >=   -> 304
      4. Range request                      -> 206 (or 416 if unsatisfiable)
      5. Full body                          -> 200

    extra_headers (e.g. {"Vary": "Accept"}) are included on all responses.
    """
    base: dict[str, str] = {
        "ETag": etag,
        "Last-Modified": last_modified,
        **(extra_headers or {}),
    }

    # -- Conditional checks --------------------------------------------------
    inm = request.headers.get("if-none-match", "")
    if inm:
        if _etag_matches(inm, etag):
            return Response(status_code=304, headers=base)
        # INM present but no match -> IMS must be ignored (RFC 7232 §3.3)
    else:
        ims_str = request.headers.get("if-modified-since", "")
        if ims_str:
            ims = parsedate(ims_str)
            lm = parsedate(last_modified)
            if ims is not None and lm is not None and ims >= lm:
                return Response(status_code=304, headers={"Last-Modified": last_modified})

    # -- Range ---------------------------------------------------------------
    total = len(body)
    start, end = 0, total
    status = 200
    range_extra: dict[str, str] = {}

    range_hdr = request.headers.get("range", "")
    if range_hdr.startswith("bytes="):
        spec = range_hdr[len("bytes=") :]
        start_s, _, end_s = spec.partition("-")
        start = int(start_s) if start_s else max(0, total - int(end_s))
        end = (int(end_s) + 1) if end_s else total
        end = min(end, total)
        if start >= total:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{total}"})
        status = 206
        range_extra["Content-Range"] = f"bytes {start}-{end - 1}/{total}"

    return Response(
        content=body[start:end],
        status_code=status,
        media_type="application/octet-stream",
        headers={**base, **range_extra, "Content-Length": str(end - start)},
    )


# ---------------------------------------------------------------------------
# Session management routes
# ---------------------------------------------------------------------------


@app.put("/tests/{session_id}", status_code=201)
def create_session(session_id: UUID, sessions: SessionsDict) -> dict[str, str]:
    """Create (or recreate) a test session."""
    sessions[session_id] = Session(id=session_id)
    return {"session_id": str(session_id)}


@app.patch("/tests/{session_id}")
def update_session(
    patch: SessionSettingsPatch,
    session: Annotated[Session, Depends(get_session)],
) -> SessionSettings:
    """Partially update session settings - only submitted (non-None) fields are applied."""
    updates = {k: v for k, v in patch.model_dump().items() if v is not None}
    session.settings = session.settings.model_copy(update=updates)
    return session.settings


@app.delete("/tests/{session_id}", status_code=204)
def delete_session(session_id: UUID, sessions: SessionsDict) -> None:
    """Destroy a session and discard its log."""
    sessions.pop(session_id, None)


# ---------------------------------------------------------------------------
# Log route
# ---------------------------------------------------------------------------


@app.get("/logs/{session_id}")
def get_log(session: Annotated[Session, Depends(get_session)]) -> list[LogEntry]:
    """Return and clear the request log for this session."""
    log, session.log = session.log, []
    return log


# ---------------------------------------------------------------------------
# Resource routes
# ---------------------------------------------------------------------------


@app.get("/tests/{session_id}/resource")
def resource(request: Request, session: SessionDep) -> Response:
    """
    Cacheable resource whose body varies on the Accept header.

    Body is the raw Accept value as bytes, or RESOURCE_BODY when Accept is absent.
    ETag is derived from the Accept value so revalidation works correctly per variant.
    Vary: Accept tells caches to key on the header.
    """
    accept = request.headers.get("accept", "")
    is_specific = bool(accept) and accept != "*/*"
    body = accept.encode() if is_specific else RESOURCE_BODY
    etag = f'"{accept}"' if is_specific else RESOURCE_ETAG
    return conditional_or_serve(
        request,
        body,
        etag,
        RESOURCE_LM,
        extra_headers={"Vary": "Accept"},
    )


@app.get("/tests/{session_id}/resource/v2")
def resource_v2(request: Request, session: SessionDep) -> Response:
    """Updated body + ETag - used by cache-invalidation tests."""
    return conditional_or_serve(request, V2_BODY, V2_ETAG, V2_LM)


@app.get("/tests/{session_id}/blob/{what}")
def blob(what: str, request: Request, session: SessionDep) -> Response:
    """
    Large binary body, fully buffered in memory at import time.

    Supported *what* values:
        npython  - Python interpreter binary x10 (~80 MB)

    ETag and Last-Modified come from session settings so tests can control
    revalidation behaviour via PATCH /tests/{session_id}.
    """
    body = BLOBS.get(what)
    if body is None:
        raise HTTPException(status_code=404, detail=f"unknown blob '{what}'")

    return conditional_or_serve(
        request,
        body,
        session.settings.blob_etag,
        session.settings.blob_last_modified,
    )
