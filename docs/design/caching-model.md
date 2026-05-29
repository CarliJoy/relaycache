# Caching Model

## Two-layer architecture

Relaycache separates the cache into two layers:

```
┌─────────────────────────────────────────────────────┐
│  In-memory index (moka LRU)                         │
│  key → CacheEntry { etag, last_modified, headers,   │
│                     blob_sha256 }                   │
│  Fast O(1) lookup; bounded by --cache-max-entries   │
└──────────────────────────┬──────────────────────────┘
                           │ blob_sha256
┌──────────────────────────▼──────────────────────────┐
│  Disk blob store                                     │
│  cache-dir/blobs/sha256/ab/ab3f7c...                │
│  Files named by SHA-256 digest of content           │
│  Two-level directory (first 2 hex chars)            │
└─────────────────────────────────────────────────────┘
```

**In-memory index** holds only metadata: validators, headers to replay,
and the blob reference. This is small (a few hundred bytes per entry) and
always fits in RAM.

**Disk blobs** hold the actual response bodies. They are never loaded into
RAM except when being served. Large responses (e.g. Docker layer blobs of
several hundred MB) are streamed from disk to the client without buffering.

## Persistence

The in-memory index is backed by a SQLite database (`proxy.db` in
`--cache-dir`). On startup, all rows are loaded into moka. During operation,
writes flow through an async background channel to a dedicated writer task —
request handling never blocks on SQLite I/O.

On clean shutdown:
1. The writer task drains its queue
2. `PRAGMA wal_checkpoint(TRUNCATE)` is called to flush WAL to the main file
3. The process exits

On restart after a crash:
1. SQLite replays any committed WAL transactions automatically
2. Relaycache scans `blobs/sha256/` and removes any blob files not recorded
   in the `blobs` table (orphans from an interrupted write)

## Blob deduplication

Bodies are stored by SHA-256 digest. If two different cache entries happen
to have identical bodies (e.g. two Vary variants that return the same bytes
for different `Accept` values), they naturally share the same blob file
without any explicit logic. The `blobs` table reference-counts this:

```sql
-- How many entries point to a given blob?
SELECT COUNT(*) FROM cache_entries WHERE blob_sha256 = ?;
```

When a cache entry is evicted, its blob is deleted only if no other entry
references it.

## Eviction

The background eviction job runs every `--eviction-interval`. It:

1. Finds entries where `accessed_at < now - entry_ttl`
2. Removes them from moka and queues DB deletes
3. After all entry deletes are committed, finds orphaned blobs:
   ```sql
   SELECT sha256 FROM blobs
   WHERE sha256 NOT IN (SELECT blob_sha256 FROM cache_entries);
   ```
4. Deletes the blob files and their `blobs` table rows

TTL is based on `accessed_at` (LRU-style): accessing a cached entry resets
its TTL. This keeps frequently-used entries alive indefinitely while
evicting stale ones.

## Database schema

```sql
-- One row per unique body (reference-counted).
CREATE TABLE blobs (
    sha256      TEXT PRIMARY KEY,
    size_bytes  INTEGER NOT NULL
);

-- One row per cached resource variant.
-- Never stores Authorization, WWW-Authenticate, Set-Cookie,
-- Proxy-Authorization, Proxy-Authenticate, or hop-by-hop headers.
CREATE TABLE cache_entries (
    key           TEXT PRIMARY KEY,
    blob_sha256   TEXT NOT NULL REFERENCES blobs(sha256),
    etag          TEXT,
    last_modified TEXT,
    headers_json  TEXT NOT NULL,   -- safe response headers only (see above)
    created_at    INTEGER NOT NULL,
    accessed_at   INTEGER NOT NULL
);

CREATE INDEX idx_entries_accessed ON cache_entries(accessed_at);
CREATE INDEX idx_entries_blob     ON cache_entries(blob_sha256);
```

### Why store `headers_json`?

When serving a cached body as `200 OK`, Relaycache must replay the original
response headers to the client: `Content-Type`, `ETag`, `Last-Modified`,
`Cache-Control`, `Docker-Content-Digest`, etc. Without these the client
receives a body with no useful metadata.

**Headers explicitly excluded from storage:**

| Header | Reason |
|--------|--------|
| `Authorization` | Request header; should never appear in responses, but excluded defensively |
| `WWW-Authenticate` | Tells clients how to authenticate; varies per-request, not a body property |
| `Set-Cookie` | Session tokens; replaying to a different user is a security violation |
| `Proxy-Authorization` | Request credential header |
| `Proxy-Authenticate` | Proxy auth challenge |
| `Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Trailers`, `Upgrade` | Hop-by-hop; connection-specific |
| `Age` | Would be stale on replay |
