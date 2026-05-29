# Persistent Storage

## SQLite WAL mode

Relaycache uses SQLite in **Write-Ahead Log (WAL)** mode for its metadata
database.

### How WAL works

In standard SQLite (rollback journal mode), writes modify the database file
directly and block concurrent readers. WAL inverts this:

```
Write:      append new page versions to proxy.db-wal
            (proxy.db is never modified during a write)

Read:       check proxy.db-wal for recent versions first,
            then fall back to proxy.db
            → readers always see a consistent committed snapshot
            → readers never block writers; writers never block readers

Checkpoint: periodically copy WAL pages back into proxy.db
            → keeps proxy.db-wal from growing indefinitely
```

### Crash safety

- On crash during a write: the incomplete WAL transaction is discarded on
  next open; proxy.db is untouched and consistent.
- On crash after a commit: the committed WAL transaction is replayed on
  next open; no data loss for committed writes.
- On clean shutdown: Relaycache calls `PRAGMA wal_checkpoint(TRUNCATE)` which
  folds all WAL pages into proxy.db and resets the WAL file to zero bytes.

### The three database files

| File | Purpose | Safe to delete? |
|------|---------|-----------------|
| `proxy.db` | Main database | Only when stopped + WAL checkpointed |
| `proxy.db-wal` | Write-ahead log | Never while running |
| `proxy.db-shm` | Shared memory index | Never while running |

After a clean shutdown, `proxy.db-wal` and `proxy.db-shm` are empty or
absent. It is then safe to copy or back up `proxy.db` alone.

## Async write pipeline

Relaycache never blocks a request handler on SQLite I/O. All database writes
flow through a `tokio::sync::mpsc` channel to a single dedicated writer task:

```
Request handler          Writer task
      │                       │
      │  DbOp::InsertEntry    │
      ├──────────────────────►│  INSERT INTO cache_entries ...
      │                       │
      │  DbOp::UpdateAccess   │
      ├──────────────────────►│  UPDATE cache_entries SET accessed_at ...
      │                       │
      │  DbOp::DeleteEntry    │
      ├──────────────────────►│  DELETE FROM cache_entries WHERE key = ?
      │                       │
      │  DbOp::DeleteBlob     │
      ├──────────────────────►│  DELETE FROM blobs WHERE sha256 = ?
      │                       │  + remove file from disk
      │  DbOp::Shutdown       │
      ├──────────────────────►│  drain queue
                              │  PRAGMA wal_checkpoint(TRUNCATE)
                              │  close connection
```

The channel is bounded (back-pressure prevents unbounded queue growth under
write storms) but large enough that normal operation never blocks.

## Startup sequence

```
1. Open (or create) proxy.db with WAL mode
2. Run migrations (CREATE TABLE IF NOT EXISTS ...)
3. Scan blobs/ directory; delete any file whose SHA256 is not in blobs table
   (orphans from a previous crash)
4. SELECT * FROM cache_entries JOIN blobs; load into moka
5. Start background writer task
6. Start background eviction task
7. Start HTTP listener
```

## Shutdown sequence

```
1. Stop accepting new connections (axum graceful shutdown)
2. Wait for in-flight requests to complete
3. Send DbOp::Shutdown to writer task
4. Wait for writer task to finish (drains queue + checkpoint)
5. Exit
```

## Blob storage layout

Bodies are stored as files named by their full SHA-256 hex digest, in a
two-level directory structure:

```
cache-dir/blobs/sha256/
  ab/ab3f7c9d...  (64 hex chars)
  ab/ab91f0e2...
  cd/cd44a1b7...
```

The first two hex characters become a subdirectory. This bounds the number
of files per directory to at most 256 subdirectories × (entries / 256) files,
which keeps filesystem metadata lookups fast even with tens of thousands of
blobs.

This layout is deliberately identical to Docker's registry blob storage,
making it easy to share or inspect blobs with standard registry tooling.

## Reference counting and GC

The `blobs` table acts as a reference count:

```sql
-- How many entries point to this blob?
SELECT COUNT(*) FROM cache_entries WHERE blob_sha256 = ?;
```

A blob is deleted when this count reaches zero. This happens:
- During the eviction job (after expiring entries are removed)
- Immediately when a cache entry is explicitly invalidated

The GC query run by the eviction job:

```sql
SELECT sha256 FROM blobs
WHERE sha256 NOT IN (SELECT blob_sha256 FROM cache_entries);
```

For each returned SHA256: delete the file, then delete the blobs row.
The index on `cache_entries.blob_sha256` makes this query fast even with
large tables.
