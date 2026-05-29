//! Persistent cache store: SQLite metadata index + content-addressed blob files.
//!
//! # Architecture
//!
//! ```text
//! CacheStore (public API)
//!   ├── moka::Cache<String, Arc<CacheEntry>>   in-memory index (fast path)
//!   ├── DbWriter (background task)             async SQLite writes
//!   └── blob files on disk                     content-addressed bodies
//!        cache-dir/blobs/sha256/<2-char>/<64-char>
//! ```
//!
//! All request handlers interact with moka only — no SQLite on the hot path.
//! SQLite writes are fire-and-forget via an unbounded channel to DbWriter.
//!
//! # Headers stored in `headers_json`
//!
//! Only safe response headers are stored.  The following are **never** stored:
//! - `Authorization`, `Proxy-Authorization` — credentials
//! - `WWW-Authenticate`, `Proxy-Authenticate` — auth challenges
//! - `Set-Cookie` — session tokens
//! - Hop-by-hop headers (`Connection`, `Transfer-Encoding`, …)
//! - `Age` — would be stale on replay
//!
//! `headers_json` is a JSON array of `["name", "value"]` pairs, preserving
//! insertion order and duplicate header names.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use moka::future::Cache;
use rusqlite::{Connection, params};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Aggregate statistics returned by the health endpoint.
pub struct StoreStats {
    pub entries: u64,
    pub blobs: u64,
    pub blob_bytes: u64,
}

/// Metadata for a single cached resource variant.
/// Bodies are never held in memory here — only the blob reference.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// `ETag` from the original 200 response (revalidation validator).
    pub etag: Option<HeaderValue>,
    /// `Last-Modified` from the original 200 response (revalidation validator).
    pub last_modified: Option<HeaderValue>,
    /// Safe response headers to replay when serving from cache.
    pub headers: HeaderMap,
    /// SHA-256 hex digest of the body blob.
    pub blob_sha256: String,
    /// Body size in bytes.
    pub blob_size: u64,
}

// ---------------------------------------------------------------------------
// Header safety list
// ---------------------------------------------------------------------------

/// Headers that must never be stored in the database.
/// This list is checked on every write and is intentionally conservative.
const UNSAFE_RESPONSE_HEADERS: &[&str] = &[
    // Credentials / auth — must never be replayed to a different user
    "authorization",
    "proxy-authorization",
    "www-authenticate",
    "proxy-authenticate",
    "set-cookie",
    // Hop-by-hop — connection-specific, invalid on replay
    "connection",
    "keep-alive",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    // Stale on replay
    "age",
];

/// Filter a response HeaderMap to only safe-to-store headers.
pub fn safe_response_headers(src: &HeaderMap) -> HeaderMap {
    let mut dst = HeaderMap::new();
    for (name, value) in src {
        if !UNSAFE_RESPONSE_HEADERS.contains(&name.as_str().to_ascii_lowercase().as_str()) {
            dst.append(name, value.clone());
        }
    }
    dst
}

// ---------------------------------------------------------------------------
// Database operations (sent over channel to writer task)
// ---------------------------------------------------------------------------

enum DbOp {
    Upsert {
        key: String,
        blob_sha256: String,
        blob_size: u64,
        etag: Option<String>,
        last_modified: Option<String>,
        headers_json: String,
        now: i64,
    },
    TouchAccess {
        key: String,
        now: i64,
    },
    DeleteEntry {
        key: String,
    },
    /// Delete blobs not referenced by any cache entry.
    /// Runs inside the writer task so it executes after all DeleteEntry ops
    /// from the same eviction cycle have been committed.
    DeleteOrphanBlobs {
        blob_root: PathBuf,
    },
    /// Return (blob_count, total_blob_bytes) to the caller.
    GetStats {
        tx: oneshot::Sender<(u64, u64)>,
    },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Temp file naming
// ---------------------------------------------------------------------------

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Return a unique temp file path inside `blob_root` for streaming downloads.
pub fn make_temp_path(blob_root: &Path) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    blob_root.join(format!(".tmp-{n}"))
}

// ---------------------------------------------------------------------------
// CacheStore
// ---------------------------------------------------------------------------

type EntryRow = (String, String, u64, Option<String>, Option<String>, String);

#[derive(Clone)]
pub struct CacheStore {
    /// In-memory index — the hot path for every request.
    pub index: Cache<String, Arc<CacheEntry>>,
    /// Blob directory root (cache_dir/blobs/sha256/).
    blob_root: PathBuf,
    /// Channel to the background DB writer.
    db_tx: mpsc::Sender<DbOp>,
}

impl CacheStore {
    /// Open the store, initialise the schema, load the index from DB,
    /// and clean up any orphaned blob files left from a previous run.
    pub async fn open(
        cache_dir: &Path,
        max_entries: u64,
    ) -> Result<(Self, tokio::task::JoinHandle<()>)> {
        let blob_root = cache_dir.join("blobs").join("sha256");
        tokio::fs::create_dir_all(&blob_root)
            .await
            .with_context(|| format!("creating blob dir {}", blob_root.display()))?;

        let db_path = cache_dir.join("proxy.db");
        let (tx, rx) = mpsc::channel::<DbOp>(4096);

        // Do all synchronous DB work (open, migrate, read) in one blocking thread.
        // rusqlite::Connection is Send but !Sync, so we can't hold &Connection
        // across async await points.
        let (conn, entries, known_blobs) = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || -> Result<(Connection, Vec<EntryRow>, HashSet<String>)> {
                let conn = open_db(&db_path)?;
                let entries = read_all_entries(&conn)?;
                let known = read_known_blobs(&conn)?;
                Ok((conn, entries, known))
            }
        })
        .await
        .context("spawning DB init task")??;

        // Populate the in-memory index from the loaded rows.
        let index: Cache<String, Arc<CacheEntry>> =
            Cache::builder().max_capacity(max_entries).build();
        let mut count = 0usize;
        for (key, blob_sha256, blob_size, etag, last_modified, headers_json) in entries {
            let headers = json_to_headers(&headers_json);
            let entry = Arc::new(CacheEntry {
                etag: etag.and_then(|s| HeaderValue::from_str(&s).ok()),
                last_modified: last_modified.and_then(|s| HeaderValue::from_str(&s).ok()),
                headers,
                blob_sha256,
                blob_size,
            });
            index.insert(key, entry).await;
            count += 1;
        }
        info!(entries = count, "loaded cache index from database");

        // Remove any blob files not referenced by the DB (from an unclean shutdown).
        cleanup_orphaned_blobs(&blob_root, known_blobs).await?;

        let handle = tokio::spawn(db_writer_task(conn, rx));

        Ok((
            CacheStore {
                index,
                blob_root,
                db_tx: tx,
            },
            handle,
        ))
    }

    /// The root directory where blobs are stored.
    pub fn blob_root(&self) -> &Path {
        &self.blob_root
    }

    /// Store a body that was streamed to `temp_path` on disk.
    ///
    /// If the blob already exists (content dedup), the temp file is removed.
    /// Otherwise the temp file is renamed atomically to its final blob path.
    /// Caller must check `vary_prevents_caching` before calling this method.
    pub async fn insert_from_disk(
        &self,
        key: &str,
        sha256: String,
        size: u64,
        temp_path: &Path,
        response_headers: &HeaderMap,
    ) -> Result<()> {
        let dest = blob_path(&self.blob_root, &sha256);
        if dest.exists() {
            // Identical content already stored; discard the temp copy.
            let _ = tokio::fs::remove_file(temp_path).await;
        } else {
            let dir = dest.parent().unwrap();
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("creating blob dir {}", dir.display()))?;
            tokio::fs::rename(temp_path, &dest)
                .await
                .with_context(|| "renaming temp file to blob")?;
            debug!(sha256, bytes = size, "wrote blob");
        }
        self.index_entry(key, sha256, size, response_headers).await;
        Ok(())
    }

    /// Insert an index entry for a blob that already exists on disk.
    /// Used to add a secondary cache key (e.g. base key for Vary discovery).
    pub async fn insert_blob_ref(
        &self,
        key: &str,
        sha256: String,
        size: u64,
        response_headers: &HeaderMap,
    ) {
        self.index_entry(key, sha256, size, response_headers).await;
    }

    /// Internal: update the moka index and fire a DB Upsert for the given key.
    async fn index_entry(
        &self,
        key: &str,
        sha256: String,
        size: u64,
        response_headers: &HeaderMap,
    ) {
        let safe = safe_response_headers(response_headers);
        let headers_json = headers_to_json(&safe);
        let etag = response_headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let last_modified = response_headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let now = unix_now();
        let entry = Arc::new(CacheEntry {
            etag: response_headers.get("etag").cloned(),
            last_modified: response_headers.get("last-modified").cloned(),
            headers: safe,
            blob_sha256: sha256.clone(),
            blob_size: size,
        });
        self.index.insert(key.to_owned(), entry).await;
        self.send_db(DbOp::Upsert {
            key: key.to_owned(),
            blob_sha256: sha256,
            blob_size: size,
            etag,
            last_modified,
            headers_json,
            now,
        });
    }

    /// Return aggregate cache statistics.  Sends a request to the DB writer
    /// and awaits the reply, so this call may block briefly under high write load.
    pub async fn stats(&self) -> StoreStats {
        let entries = self.index.entry_count();
        let (tx, rx) = oneshot::channel();
        self.send_db(DbOp::GetStats { tx });
        let (blobs, blob_bytes) = rx.await.unwrap_or((0, 0));
        StoreStats {
            entries,
            blobs,
            blob_bytes,
        }
    }

    /// Look up an entry by key.  Updates `accessed_at` in DB on hit.
    pub async fn get(&self, key: &str) -> Option<Arc<CacheEntry>> {
        let entry = self.index.get(key).await?;
        self.send_db(DbOp::TouchAccess {
            key: key.to_owned(),
            now: unix_now(),
        });
        Some(entry)
    }

    /// Open a blob file for streaming.
    pub async fn open_blob(&self, sha256: &str) -> Result<tokio::fs::File> {
        let path = blob_path(&self.blob_root, sha256);
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("opening blob {}", path.display()))
    }

    /// Remove a cache entry (called by eviction job).
    pub async fn evict(&self, key: &str) {
        self.index.invalidate(key).await;
        self.send_db(DbOp::DeleteEntry {
            key: key.to_owned(),
        });
    }

    /// Signal the DB writer to shut down cleanly (drain + checkpoint).
    pub fn shutdown(&self) {
        self.send_db(DbOp::Shutdown);
    }

    fn send_db(&self, op: DbOp) {
        if self.db_tx.try_send(op).is_err() {
            warn!("DB writer channel full; dropping write op");
        }
    }
}

// ---------------------------------------------------------------------------
// Blob file helpers
// ---------------------------------------------------------------------------

fn blob_path(root: &Path, sha256: &str) -> PathBuf {
    // Two-level directory: first 2 hex chars / full 64-char filename.
    root.join(&sha256[..2]).join(sha256)
}

#[cfg(test)]
fn sha256_hex(data: &bytes::Bytes) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

// ---------------------------------------------------------------------------
// SQLite helpers
// ---------------------------------------------------------------------------

fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;

    // WAL mode: readers never block writers; writers never block readers.
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute_batch("PRAGMA synchronous=NORMAL;")?; // safe with WAL
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS blobs (
            sha256      TEXT PRIMARY KEY,
            size_bytes  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cache_entries (
            key           TEXT PRIMARY KEY,
            blob_sha256   TEXT NOT NULL REFERENCES blobs(sha256),
            etag          TEXT,
            last_modified TEXT,
            -- Safe response headers only.  See UNSAFE_RESPONSE_HEADERS.
            -- Format: JSON array of [\"name\", \"value\"] pairs.
            headers_json  TEXT NOT NULL,
            created_at    INTEGER NOT NULL,
            accessed_at   INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_entries_accessed
            ON cache_entries(accessed_at);

        CREATE INDEX IF NOT EXISTS idx_entries_blob
            ON cache_entries(blob_sha256);
    ",
    )?;

    Ok(conn)
}

fn read_all_entries(conn: &Connection) -> Result<Vec<EntryRow>> {
    let mut stmt = conn.prepare(
        "SELECT e.key, e.blob_sha256, b.size_bytes, e.etag, e.last_modified, e.headers_json
         FROM cache_entries e
         JOIN blobs b ON b.sha256 = e.blob_sha256",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, u64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    rows.map(|r| r.context("reading entry row")).collect()
}

fn read_known_blobs(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT sha256 FROM blobs")?;
    let known = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(known)
}

async fn cleanup_orphaned_blobs(blob_root: &Path, known: HashSet<String>) -> Result<()> {
    let mut removed = 0usize;
    let Ok(mut top) = tokio::fs::read_dir(blob_root).await else {
        return Ok(());
    };
    loop {
        let Ok(Some(entry)) = top.next_entry().await else {
            break;
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".tmp-") {
            // Leftover temp file from an unclean shutdown.
            let _ = tokio::fs::remove_file(entry.path()).await;
            removed += 1;
            continue;
        }
        // Dive into a 2-char prefix subdirectory.
        let Ok(mut sub_dir) = tokio::fs::read_dir(entry.path()).await else {
            continue;
        };
        loop {
            let Ok(Some(blob_entry)) = sub_dir.next_entry().await else {
                break;
            };
            let blob_name = blob_entry.file_name().to_string_lossy().into_owned();
            if !known.contains(&blob_name) {
                let _ = tokio::fs::remove_file(blob_entry.path()).await;
                removed += 1;
            }
        }
    }
    if removed > 0 {
        info!(removed, "cleaned up orphaned blob files");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Background DB writer task
// ---------------------------------------------------------------------------

async fn db_writer_task(conn: Connection, mut rx: mpsc::Receiver<DbOp>) {
    while let Some(op) = rx.recv().await {
        match op {
            DbOp::Upsert {
                key,
                blob_sha256,
                blob_size,
                etag,
                last_modified,
                headers_json,
                now,
            } => {
                let r = conn.execute(
                    "INSERT OR IGNORE INTO blobs (sha256, size_bytes) VALUES (?1, ?2)",
                    params![blob_sha256, blob_size],
                );
                if let Err(e) = r {
                    warn!(error = %e, "DB insert blob");
                }

                let r = conn.execute(
                    "INSERT INTO cache_entries \
                     (key, blob_sha256, etag, last_modified, headers_json, created_at, accessed_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                     ON CONFLICT(key) DO UPDATE SET
                       blob_sha256   = excluded.blob_sha256,
                       etag          = excluded.etag,
                       last_modified = excluded.last_modified,
                       headers_json  = excluded.headers_json,
                       accessed_at   = excluded.accessed_at",
                    params![key, blob_sha256, etag, last_modified, headers_json, now],
                );
                if let Err(e) = r {
                    warn!(error = %e, "DB upsert entry");
                }
            }

            DbOp::TouchAccess { key, now } => {
                let r = conn.execute(
                    "UPDATE cache_entries SET accessed_at = ?1 WHERE key = ?2",
                    params![now, key],
                );
                if let Err(e) = r {
                    warn!(error = %e, "DB touch access");
                }
            }

            DbOp::DeleteEntry { key } => {
                let r = conn.execute("DELETE FROM cache_entries WHERE key = ?1", params![key]);
                if let Err(e) = r {
                    warn!(error = %e, "DB delete entry");
                }
            }

            DbOp::DeleteOrphanBlobs { blob_root } => {
                let mut stmt = match conn.prepare(
                    "SELECT sha256 FROM blobs \
                     WHERE sha256 NOT IN (SELECT blob_sha256 FROM cache_entries)",
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "DB query orphan blobs");
                        continue;
                    }
                };
                let orphans: Vec<String> = match stmt.query_map([], |r| r.get(0)) {
                    Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                    Err(e) => {
                        warn!(error = %e, "DB iterate orphan blobs");
                        continue;
                    }
                };
                let mut removed = 0usize;
                for sha256 in &orphans {
                    let _ = fs::remove_file(blob_path(&blob_root, sha256));
                    if let Err(e) =
                        conn.execute("DELETE FROM blobs WHERE sha256 = ?1", params![sha256])
                    {
                        warn!(error = %e, "DB delete orphan blob");
                    } else {
                        removed += 1;
                    }
                }
                if removed > 0 {
                    info!(orphaned_blobs = removed, "cleaned up orphaned blobs");
                }
            }

            DbOp::GetStats { tx } => {
                let result = conn
                    .query_row(
                        "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM blobs",
                        [],
                        |r| Ok((r.get::<_, u64>(0)?, r.get::<_, u64>(1)?)),
                    )
                    .unwrap_or((0, 0));
                let _ = tx.send(result);
            }

            DbOp::Shutdown => {
                // Drain remaining messages, processing entry deletions only.
                while let Ok(op) = rx.try_recv() {
                    if let DbOp::DeleteEntry { key } = op {
                        let _ =
                            conn.execute("DELETE FROM cache_entries WHERE key = ?1", params![key]);
                    }
                }
                // Checkpoint WAL → main DB file.
                match conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                    Err(e) => {
                        warn!(error = %e, "WAL checkpoint failed");
                    }
                    _ => {
                        info!("WAL checkpoint complete");
                    }
                }
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Eviction job
// ---------------------------------------------------------------------------

/// Background task: periodically evict expired entries and orphaned blobs.
pub async fn eviction_task(
    store: CacheStore,
    entry_ttl: Duration,
    interval: Duration,
    blob_root: PathBuf,
    db_path: PathBuf,
) {
    loop {
        tokio::time::sleep(interval).await;
        let result = run_eviction(&store, entry_ttl, &blob_root, &db_path).await;
        if let Err(e) = result {
            warn!(error = %e, "eviction job error");
        }
    }
}

async fn run_eviction(
    store: &CacheStore,
    entry_ttl: Duration,
    blob_root: &Path,
    db_path: &Path,
) -> Result<()> {
    let cutoff = unix_now() - entry_ttl.as_secs() as i64;

    // Read expired keys via a separate read-only connection.
    let expired_keys = tokio::task::spawn_blocking({
        let db_path = db_path.to_owned();
        move || -> Result<Vec<String>> {
            let conn = Connection::open(&db_path)
                .with_context(|| format!("opening eviction DB {}", db_path.display()))?;
            let mut stmt = conn.prepare("SELECT key FROM cache_entries WHERE accessed_at < ?1")?;
            let keys = stmt
                .query_map(params![cutoff], |r| r.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(keys)
        }
    })
    .await??;

    let expired_count = expired_keys.len();
    for key in &expired_keys {
        store.evict(key).await;
    }

    // Orphan blob cleanup runs in the writer task, after all DeleteEntry ops
    // from this cycle have been committed, so blobs orphaned this cycle are
    // found and removed immediately rather than waiting for the next cycle.
    store.send_db(DbOp::DeleteOrphanBlobs {
        blob_root: blob_root.to_owned(),
    });

    if expired_count > 0 {
        info!(expired = expired_count, "evicted expired cache entries");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Header serialisation
// ---------------------------------------------------------------------------

fn headers_to_json(headers: &HeaderMap) -> String {
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
        .collect();
    serde_json::to_string(&pairs).unwrap_or_else(|_| "[]".to_owned())
}

fn json_to_headers(json: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let pairs: Vec<(String, String)> = serde_json::from_str(json).unwrap_or_default();
    for (name, value) in pairs {
        let Ok(n) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(v) = HeaderValue::from_str(&value) else {
            continue;
        };
        headers.append(n, v);
    }
    headers
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn safe_headers_strips_auth() {
        let mut src = HeaderMap::new();
        src.insert("authorization", HeaderValue::from_static("Bearer secret"));
        src.insert("content-type", HeaderValue::from_static("application/json"));
        src.insert("set-cookie", HeaderValue::from_static("session=abc"));
        src.insert(
            "www-authenticate",
            HeaderValue::from_static("Bearer realm=test"),
        );
        let safe = safe_response_headers(&src);
        assert!(
            safe.get("authorization").is_none(),
            "authorization must be stripped"
        );
        assert!(
            safe.get("set-cookie").is_none(),
            "set-cookie must be stripped"
        );
        assert!(
            safe.get("www-authenticate").is_none(),
            "www-authenticate must be stripped"
        );
        assert!(
            safe.get("content-type").is_some(),
            "content-type must be kept"
        );
    }

    #[test]
    fn headers_roundtrip() {
        let mut src = HeaderMap::new();
        src.insert("content-type", HeaderValue::from_static("application/json"));
        src.insert("etag", HeaderValue::from_static(r#""v1""#));
        let json = headers_to_json(&src);
        let recovered = json_to_headers(&json);
        assert_eq!(
            recovered.get("content-type").map(|v| v.to_str().unwrap()),
            Some("application/json")
        );
        assert_eq!(
            recovered.get("etag").map(|v| v.to_str().unwrap()),
            Some(r#""v1""#)
        );
    }

    #[test]
    fn headers_roundtrip_multi_value() {
        let mut src = HeaderMap::new();
        src.append("x-custom", HeaderValue::from_static("a"));
        src.append("x-custom", HeaderValue::from_static("b"));
        let json = headers_to_json(&src);
        let recovered = json_to_headers(&json);
        let values: Vec<&str> = recovered
            .get_all("x-custom")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(values, ["a", "b"]);
    }

    #[test]
    fn sha256_hex_deterministic() {
        let data = Bytes::from("hello world");
        let a = sha256_hex(&data);
        let b = sha256_hex(&data);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn blob_path_layout() {
        let root = PathBuf::from("/cache/blobs/sha256");
        let sha = "ab3f7c9d".repeat(8); // 64 chars
        let path = blob_path(&root, &sha);
        assert!(path.to_string_lossy().contains("/ab/"));
        assert!(path.file_name().unwrap() == sha.as_str());
    }
}
