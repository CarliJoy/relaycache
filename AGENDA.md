# Relaycache — development agenda

See [`docs/`](docs/) for project documentation and [`agents.md`](agents.md)
for the development workflow.

---

## TODO

### 🟡 Should do before production use

1. **`Content-Encoding` + Range** — if the upstream sends a gzip-encoded body
   and the client requests a byte range, offsets apply to compressed bytes.
   Either document the limitation, decompress before slicing, or don't cache
   gzip-encoded bodies for range purposes.

---

### 🟢 Nice to have

2. **`--tls-cert` / `--tls-key`** — TLS termination via `axum-server` + rustls.

3. **`--upstream-ca`** — custom CA for upstream TLS (corporate registries).
   The default is the system CA store (via `rustls-native-certs`).

4. **`PURGE` method** — `PURGE /path` endpoint protected by `--purge-token`
   for explicit cache invalidation.

5. **Docker Compose example** — `docker-compose.yml` showing relaycache in
   front of a local registry mirror.
