"""
Upstream - session-scoped client wrapper around the fake FastAPI upstream.

Usage in fixtures
-----------------
    @pytest.fixture()
    def upstream(upstream_server_url: str) -> Generator[Upstream, None, None]:
        up = Upstream.create(upstream_server_url)
        yield up
        up.close()

The Upstream object is what tests interact with:

    upstream.set_auth(required=True, token="secret")
    upstream.reset_settings()
    entries = upstream.drain_log()
    entries[0].header("via")

base_url is /tests/{session_id} - point relaycache at this so all proxied
requests land in the right session.  The management client is internal.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import httpx
from upstream_app import LogEntry, SessionSettings, SessionSettingsPatch


class Upstream:
    """
    Session-scoped handle to one test session on the fake upstream server.

    base_url  ->  http://<host>/tests/<session_id>
    """

    def __init__(self, server_url: str, session_id: UUID, client: httpx.Client) -> None:
        self._server_url = server_url.rstrip("/")
        self._session_id = session_id
        self._client = client

    # -- Factory -------------------------------------------------------------

    @classmethod
    def create(cls, server_url: str) -> Upstream:
        """Create a new session on *server_url* and return an Upstream handle."""
        session_id = uuid4()
        client = httpx.Client(base_url=server_url)
        client.put(f"/tests/{session_id}").raise_for_status()
        return cls(server_url, session_id, client)

    # -- Properties ----------------------------------------------------------

    @property
    def session_id(self) -> UUID:
        return self._session_id

    @property
    def base_url(self) -> str:
        """Upstream base URL for relaycache.  No trailing slash."""
        return f"{self._server_url}/tests/{self._session_id}"

    # -- Settings ------------------------------------------------------------

    def patch_settings(self, **kwargs: object) -> SessionSettings:
        """
        Partially update session settings.  Only supplied kwargs are changed.

        Example:
            upstream.patch_settings(auth_required=True)
            upstream.patch_settings(blob_etag='"v2"', blob_last_modified="...")
        """
        patch = SessionSettingsPatch(**kwargs)
        resp = self._client.patch(
            f"/tests/{self._session_id}",
            content=patch.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return SessionSettings.model_validate(resp.json())

    def set_auth(self, *, required: bool, token: str = "secret") -> None:
        """Enable or disable auth enforcement for all endpoints in this session."""
        self.patch_settings(auth_required=required, valid_token=token)

    def set_blob_identity(self, *, etag: str, last_modified: str) -> None:
        """Change the ETag / Last-Modified the upstream advertises for /blob/* endpoints."""
        self.patch_settings(blob_etag=etag, blob_last_modified=last_modified)

    def update_settings(self, settings: SessionSettings) -> SessionSettings:
        """Replace all session settings with *settings*."""
        return self.patch_settings(
            auth_required=settings.auth_required,
            valid_token=settings.valid_token,
            blob_etag=settings.blob_etag,
            blob_last_modified=settings.blob_last_modified,
        )

    def reset_settings(self) -> SessionSettings:
        """Restore all session settings to their defaults by replacing the session."""
        resp = self._client.put(f"/tests/{self._session_id}")
        resp.raise_for_status()
        # PUT recreates the session with fresh defaults; return default settings
        return SessionSettings()

    # -- Log -----------------------------------------------------------------

    def drain_log(self) -> list[LogEntry]:
        """Return and clear the request log for this session."""
        resp = self._client.get(f"/logs/{self._session_id}")
        resp.raise_for_status()
        return [LogEntry.model_validate(e) for e in resp.json()]

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Delete the session from the server and close the HTTP client."""
        try:
            self._client.delete(f"/tests/{self._session_id}")
        finally:
            self._client.close()
