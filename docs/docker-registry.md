# Docker Registry Usage

Relaycache works well as a caching proxy in front of a Docker / OCI registry.
This page covers the specifics of the registry API and how Relaycache handles each
request type.

## Docker pull request sequence

A `docker pull` is not a single request. It is a structured sequence:

### 1. API version check

```
GET /v2/
→ 200 OK             (no auth required)
→ 401 Unauthorized   (with Www-Authenticate pointing to token endpoint)
```

Relaycache passes this through. It is typically not cacheable (no body worth
caching, or `Cache-Control: no-cache`).

### 2. Token fetch

The client fetches a short-lived bearer token from an auth server
(often `auth.docker.io` or a separate host). This is a **different host**
from the registry — Relaycache only proxies requests to `--upstream`, so token
fetches are not proxied at all.

### 3. Manifest fetch

```
GET /v2/<name>/manifests/<reference>
Accept: application/vnd.oci.image.manifest.v1+json, ...
Authorization: Bearer <token>
```

The reference is either a tag (`latest`) or a digest (`sha256:abc123`).

**Relaycache behaviour:**
- Caches `200` responses
- Registry sends `Vary: Accept` — Relaycache creates separate cache entries
  per `Accept` value
- ETag = content digest — validators work perfectly
- Tags are mutable; when `latest` changes, the origin returns `200` with
  a new body (new digest → new ETag) and Relaycache updates the cache

### 4. Config blob fetch

```
GET /v2/<name>/blobs/sha256:<digest>
Authorization: Bearer <token>
```

Small JSON file describing the image config. URL contains the digest → fully
content-addressed → immutable. Relaycache caches this.

### 5. Layer blob fetches

```
GET /v2/<name>/blobs/sha256:<digest>
Authorization: Bearer <token>
Range: bytes=0-10485759   (often, for large layers)
```

These can be very large (hundreds of MB). The URL contains the digest →
immutable content.

**Range handling:** On first fetch, Relaycache ignores the `Range` header and
downloads the full layer, caches it, then serves the requested range as
`206`. Subsequent range requests for the same layer are served from the
cache with a round-trip to the registry for auth only (no body re-download).

Large layers (> `--max-cacheable-size`) are passed through as-is without
caching.

## Summary table

| Request | Cached? | Notes |
|---------|---------|-------|
| `GET /v2/` | ❌ | Auth challenge, no useful body |
| Manifest by tag | ✅ | Mutable tag; ETag = digest |
| Manifest by digest | ✅ | Immutable; ETag = digest |
| Config blob | ✅ | Small, immutable |
| Layer blob (≤ max-cacheable-size) | ✅ | Full-fetch upgrade on first range |
| Layer blob (> max-cacheable-size) | ❌ | Pass-through |
| Token endpoint | ❌ | Different host, not proxied |
| `POST /v2/<name>/blobs/uploads/` | ❌ | Non-GET |
| `PUT /v2/<name>/manifests/<tag>` | ❌ | Non-GET |

## Example configuration

```bash
relaycache \
  --upstream https://registry-1.docker.io \
  --bind 0.0.0.0:5000 \
  --cache-dir /var/cache/relaycache \
  --max-cacheable-size 2GiB \
  --entry-ttl 7days \
  --eviction-interval 6h
```

Configure Docker to use the proxy:

```json
// /etc/docker/daemon.json
{
  "registry-mirrors": ["http://your-relaycache-host:5000"]
}
```

Or for a private registry, configure `--upstream` to point to it and have
clients use `your-relaycache-host:5000` as the registry address.

## Auth note

Docker clients send a fresh bearer token on every request. The token is
passed through to the registry on every request (Relaycache always forwards all
headers). The registry validates the token on every request, meaning revoked
access takes effect immediately — there is no window where a cached response
is served to an unauthorised client.
