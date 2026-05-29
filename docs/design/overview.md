# Design Overview

## Why always forward?

Conventional caching proxies serve from cache when the entry is fresh, skipping
the origin entirely. This is the whole point of a cache — but it means the
origin's access control is bypassed for cached responses. If a user's credentials
are revoked, they can still receive cached content until it expires.

Relaycache's security model is simpler and stricter:

> **The origin server is the sole authority on both content freshness and
> access control. Relaycache never serves a response body without the origin
> first confirming the request is valid.**

Every request — regardless of cache state — is forwarded to the origin with
all original headers intact (including `Authorization`, cookies, etc.). The
cache is a bandwidth-saving mechanism only.

## Request lifecycle

```
                    ┌─────────────────────────────────────┐
                    │              Relaycache                 │
                    │                                      │
Client request ────►│  1. Look up cache key               │
(with auth)         │  2. Build forwarded request:        │
                    │     - all client headers             │
                    │     - merge validators from cache    │
                    │     - strip Range if body cached     │
                    │  3. Forward to origin               │──► Origin
                    │                                      │◄── 200 / 304 / 4xx
                    │  4a. 200: store blob, return 200    │
                    │  4b. 304 + proxy hit: return 200    │──► Client
                    │       or 206 (if range requested)   │
                    │  4c. 304 + client hit: return 304   │
                    │  4d. anything else: pass through    │
                    └─────────────────────────────────────┘
```

## Headers added by Relaycache

### On forwarded requests (to origin)

| Header | Value | Purpose |
|--------|-------|---------|
| `Via` | `1.1 relaycache` | RFC 7230 §5.7.1 — identifies proxy in chain |

### On responses (to client)

| Header | Value | Purpose |
|--------|-------|---------|
| `Via` | `1.1 relaycache` | RFC 7230 §5.7.1 — identifies proxy in chain |
| `X-Cache` | `HIT` or `MISS` | Whether body came from cache or origin |
| `X-Cache-Key` | the cache key string | Debugging — shows which key was used including Vary dimensions |

## What is and is not cached

| Condition | Cached? | Reason |
|-----------|---------|--------|
| `GET` + `200 OK` | ✅ | Standard cacheable response |
| `GET` + `200 OK` + `Vary: *` | ❌ | RFC 7234: must not cache |
| `GET` + `200 OK` + `Vary: Authorization` | ❌ | User-personalised; would require per-credential storage |
| `GET` + `200 OK` + body > `--max-cacheable-size` | ❌ | Too large |
| Any non-GET method | ❌ | Only GET bodies are idempotent to cache |
| Any non-200 status | ❌ | Error responses, redirects, etc. pass through |

## Module structure

```
src/
  main.rs      Entry point: wires config → store → proxy → listener
  config.rs    CLI/env config parsing (clap, humantime, size parsing)
  headers.rs   All HTTP header logic (pure functions, well-tested)
  proxy.rs     Request handler: cache lookup → forward → store/serve
  store.rs     CacheStore: moka index + SQLite DB + blob files
```

## Key invariants

1. **Every request reaches the upstream.** Never serve a response body without
   a round-trip to the upstream first.

2. **Auth headers are never stored.** `store::safe_response_headers` strips
   `Authorization`, `Set-Cookie`, `WWW-Authenticate`, and `Proxy-*` headers.
   Add a unit test verifying this if `UNSAFE_RESPONSE_HEADERS` ever changes.

3. **`Vary: Authorization` → never cache.** Enforced in both
   `headers::vary_prevents_caching` and `store::insert`.

4. **Blob files are immutable.** Written once (content-addressed); `write_blob`
   skips writing if the file already exists.

5. **WAL checkpoint on clean shutdown.** `store::DbOp::Shutdown` triggers
   `PRAGMA wal_checkpoint(TRUNCATE)`. Do not exit without sending this.
