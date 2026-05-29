# Relaycache

Relaycache is an always-revalidating HTTP proxy with a content-addressed disk cache.

## What problem does it solve?

Standard caching proxies (nginx, Varnish, Squid) are designed to serve responses
directly from cache without contacting the origin server when a cached entry is
fresh. This is efficient but means **the origin server's authentication and
authorization logic is bypassed** for cached responses.

Relaycache inverts this: the origin server is consulted on **every single request**,
including auth checks. The cache is used only to avoid re-transmitting the
response body when the origin confirms the content has not changed (HTTP `304
Not Modified`). The origin always owns auth.

```
Client ──► Relaycache ──► Origin  (every request, full headers including Authorization)
                          │
                    200 + body  →  store body as SHA-256 blob, return 200 to client
                    304         →  serve cached body as 200 (or 206 for ranges)
                    anything else → forward as-is, do not cache
```

## Key features

- **Auth-safe caching** — origin enforces auth on every request
- **Content-addressed blob storage** — bodies stored by SHA-256, Docker-registry layout
- **Persistent cache** — survives restarts; SQLite index + blob files on disk
- **Streaming store** — bodies stream directly to disk; no full-body RAM buffering
- **Vary-aware** — correct per-variant caching; `Vary: Authorization` → never cached
- **Range request handling** — full-fetch upgrade strategy with slice-from-cache
- **Merged validators** — `max(client, proxy)` strategy for `If-Modified-Since`;
  merged ETag list for `If-None-Match`
- **Background eviction** — TTL-based GC with orphaned blob cleanup
- **Health endpoint** — `GET /__relaycache/health` for monitoring
- **Unix socket support** — for sidecar and container deployments
- **System CA trust** — uses the OS certificate store for upstream TLS
- **Standards-compliant** — adds `Via` header; uses `X-Cache` and `X-Cache-Key`
  for observability

## Quick start

```bash
cargo build --release

# Proxy to a Docker registry (cache-dir defaults to ~/.cache/relaycache/...)
./target/release/relaycache https://registry.example.com

# With explicit options
./target/release/relaycache https://registry.example.com \
  --bind 0.0.0.0:8080 \
  --cache-dir /var/cache/relaycache \
  --cache-max-entries 100000 \
  --max-cacheable-size 512MiB \
  --entry-ttl 24h \
  --eviction-interval 1h
```

## Documentation

- [Configuration reference](docs/configuration.md)
- [Design overview](docs/design/overview.md)
- [Caching model](docs/design/caching-model.md)
- [Range requests](docs/design/range-requests.md)
- [Vary header handling](docs/design/vary.md)
- [Validator merging](docs/design/validator-merging.md)
- [Persistent storage](docs/design/storage.md)
- [Docker registry usage](docs/docker-registry.md)

## A note the author

I'm a Python developer who vibe-coded this project with Rust to start learning
the language. Even so, I created integration tests that let me completely verify
the behaviour from the outside. Combined with safe Rust throughout
(`#![forbid(unsafe_code)]`), the project should be reliable enough for real use.
Feedback is very welcome — please open an issue if you find something wrong.
