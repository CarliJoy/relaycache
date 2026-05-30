# Configuration

## Usage

```
relaycache <UPSTREAM> [OPTIONS]
```

`<UPSTREAM>` is a required positional argument (or `UPSTREAM` env var): the base
URL that relaycache proxies to.

## Options

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--bind` | `BIND` | `0.0.0.0:8080` | TCP address to listen on |
| `--unix-socket` | `UNIX_SOCKET` | *unset* | Unix socket path (overrides `--bind`) |
| `--cache-dir` | `CACHE_DIR` | see below | Directory for blob files and `proxy.db` |
| `--cache-max-entries` | `CACHE_MAX_ENTRIES` | `100000` | Maximum in-memory index entries (LRU) |
| `--max-cacheable-size` | `MAX_CACHEABLE_SIZE` | `512MiB` | Bodies larger than this are never cached |
| `--entry-ttl` | `ENTRY_TTL` | `24h` | How long a cache entry lives without access |
| `--eviction-interval` | `EVICTION_INTERVAL` | `1h` | How often the background eviction job runs |

### Cache directory default

When `--cache-dir` is not set, relaycache derives a path from the upstream URL:

```
$XDG_CACHE_HOME/relaycache/<upstream-name>
```

`$XDG_CACHE_HOME` falls back to `~/.cache` when unset (XDG Base Directory
Specification). `<upstream-name>` is derived from the upstream URL by stripping
the scheme (`https://` / `http://`), then replacing every character that is not
alphanumeric, `-`, or `.` with `_`, and trimming leading/trailing underscores.

Examples:

| Upstream URL | `<upstream-name>` |
|---|---|
| `https://registry.example.com` | `registry.example.com` |
| `https://registry.example.com:5000` | `registry.example.com_5000` |
| `https://registry.example.com/v2` | `registry.example.com_v2` |
| `https://registry-1.docker.io` | `registry-1.docker.io` |

## Duration format

`--entry-ttl` and `--eviction-interval` accept human-readable durations via the
[`humantime`](https://docs.rs/humantime) crate:

```
30s        30 seconds
5min       5 minutes
1h 30min   1 hour 30 minutes
24h        24 hours
7days      7 days
```

## Size format

`--max-cacheable-size` accepts human-readable sizes:

```
100MiB     100 mebibytes (1024-based)
1GiB       1 gibibyte
512MB      512 megabytes (1000-based)
```

## Cache directory layout

```
cache-dir/
  proxy.db          SQLite database (entries + blob index)
  proxy.db-wal      SQLite WAL file (present while running)
  proxy.db-shm      SQLite shared memory (present while running)
  blobs/
    sha256/
      ab/
        ab3f7c...   body blob (full 64-char hex filename)
      cd/
        cd91a2...
```

:::{warning}
Never copy `proxy.db` without `proxy.db-wal` and `proxy.db-shm` while
relaycache is running — you will get an inconsistent snapshot.
Stop relaycache first; it will checkpoint the WAL on clean shutdown.
:::

## Logging

Relaycache uses structured logging via [`tracing`](https://docs.rs/tracing).
Set `RUST_LOG` to control verbosity:

```bash
RUST_LOG=relaycache=info      # normal operation (default)
RUST_LOG=relaycache=debug     # request-level detail
RUST_LOG=relaycache=trace     # very verbose, includes header dumps
```
