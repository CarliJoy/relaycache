//! Core proxy request handler.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, IF_RANGE, RANGE},
    },
    response::Response,
};
use bytes::Bytes;
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

use crate::{
    headers::{
        add_via, base_cache_key, did_client_validator_match, forward_headers, is_hop_by_hop,
        merge_etag_validators, merge_ims_validators, parse_range_header, vary_cache_key,
        vary_prevents_caching,
    },
    store::{CacheEntry, CacheStore, make_temp_path},
};

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub upstream: Arc<String>,
    pub client: Client,
    pub store: CacheStore,
    pub max_cacheable_size: u64,
}

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------

pub async fn health(State(state): State<AppState>) -> Response {
    let s = state.store.stats().await;
    let body = format!(
        r#"{{"entries":{},"blobs":{},"blob_bytes":{}}}"#,
        s.entries, s.blobs, s.blob_bytes
    );
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn handle(State(state): State<AppState>, req: Request) -> Response {
    match proxy_request(state, req).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "proxy error");
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from(format!("proxy error: {e}")))
                .unwrap()
        }
    }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

async fn proxy_request(state: AppState, req: Request) -> Result<Response> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let incoming = req.headers().clone();
    // Read request body into memory (only relevant for POST/PUT; GETs have none).
    let req_body = axum::body::to_bytes(req.into_body(), state.max_cacheable_size as usize)
        .await
        .context("reading request body")?;

    let is_cacheable_method = method == Method::GET;
    let client_range = incoming.get(RANGE).cloned();
    let has_range = client_range.is_some();

    // ------------------------------------------------------------------
    // Cache lookup (two-phase: base key → Vary refinement)
    // ------------------------------------------------------------------
    let base_key = base_cache_key(&method, &uri);

    let first_hit = if is_cacheable_method {
        state.store.get(&base_key).await
    } else {
        None
    };

    // Refine key using the Vary dimensions stored in the found entry.
    let (cache_key, cached) = if let Some(ref entry) = first_hit {
        let refined = vary_cache_key(&base_key, &incoming, &entry.headers);
        if refined != base_key {
            let hit2 = state.store.get(&refined).await;
            (refined, hit2)
        } else {
            (base_key, first_hit)
        }
    } else {
        (base_key, None)
    };

    // ------------------------------------------------------------------
    // Build forwarded request headers
    //
    // Always forward all client headers (auth, cookies, etc.).
    // Merge in our cached validators so the upstream can short-circuit.
    //
    // When we have the full body cached and the client sent Range/If-Range:
    //   strip them — we want a plain conditional GET so the upstream can
    //   respond 304, then we slice from cache ourselves.
    // ------------------------------------------------------------------
    let mut forwarded = forward_headers(&incoming);
    add_via(&mut forwarded); // RFC 7230 §5.7.1

    if cached.is_some() && has_range {
        forwarded.remove(RANGE);
        forwarded.remove(IF_RANGE);
        debug!(key = %cache_key, "stripping Range/If-Range — will slice from cache after auth check");
    }

    if let Some(ref entry) = cached {
        merge_etag_validators(&incoming, entry, &mut forwarded);
        merge_ims_validators(&incoming, entry, &mut forwarded);
    }

    // ------------------------------------------------------------------
    // Forward to upstream
    // ------------------------------------------------------------------
    let upstream_url = format!(
        "{}{}",
        state.upstream,
        uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
    );

    let upstream_resp = state
        .client
        .request(method.clone(), &upstream_url)
        .headers(forwarded)
        .body(req_body)
        .send()
        .await
        .context("upstream request failed")?;

    let status = upstream_resp.status();
    let up_headers = upstream_resp.headers().clone();

    debug!(
        method = %method,
        url    = %upstream_url,
        status = status.as_u16(),
        cached = cached.is_some(),
        range  = has_range,
        key    = %cache_key,
        "upstream response"
    );

    // ------------------------------------------------------------------
    // 304 Not Modified
    // ------------------------------------------------------------------
    if status == StatusCode::NOT_MODIFIED {
        if let Some(ref entry) = cached {
            let client_current = did_client_validator_match(&incoming, &up_headers);

            return if has_range && !client_current {
                // Auth check passed; proxy cache is valid; serve the range.
                info!(key = %cache_key, "304 + Range → slicing cached body");
                serve_range_from_cache(entry, &client_range, &up_headers, &cache_key, &state).await
            } else if client_current {
                // Client already has the correct version.
                info!(key = %cache_key, "304 → client cache current");
                Ok(build_passthrough_response(
                    StatusCode::NOT_MODIFIED,
                    &up_headers,
                    Bytes::new(),
                    &cache_key,
                ))
            } else {
                // Only the proxy's ETag matched; give client the full body.
                info!(key = %cache_key, "304 → proxy cache hit, returning 200");
                serve_full_from_cache(entry, &up_headers, &cache_key, &state).await
            };
        }

        // 304 with no cache entry (restart + warm DB reload not yet complete).
        warn!(key = %cache_key, "304 with no cache entry; forwarding 304");
        return Ok(build_passthrough_response(
            StatusCode::NOT_MODIFIED,
            &up_headers,
            Bytes::new(),
            &cache_key,
        ));
    }

    // ------------------------------------------------------------------
    // GET 200: stream body to disk to avoid buffering in RAM
    // ------------------------------------------------------------------
    if is_cacheable_method && status == StatusCode::OK {
        return handle_get_200(
            state,
            upstream_resp,
            up_headers,
            cache_key,
            incoming,
            client_range,
            has_range,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Everything else (non-GET, non-200, errors, redirects): buffer and
    // pass through.  These are typically small responses.
    // ------------------------------------------------------------------
    let body_bytes = upstream_resp
        .bytes()
        .await
        .context("reading upstream body")?;
    Ok(build_passthrough_response(
        status,
        &up_headers,
        body_bytes,
        &cache_key,
    ))
}

// ---------------------------------------------------------------------------
// GET 200 streaming handler
// ---------------------------------------------------------------------------

async fn handle_get_200(
    state: AppState,
    upstream_resp: reqwest::Response,
    up_headers: HeaderMap,
    cache_key: String,
    incoming: HeaderMap,
    client_range: Option<HeaderValue>,
    has_range: bool,
) -> Result<Response> {
    let blob_root = state.store.blob_root().to_owned();

    // Stream the entire body to a temp file while computing SHA-256.
    let (temp_path, sha256, actual_size) = stream_to_temp(&blob_root, upstream_resp).await?;

    // Decide whether to cache this response.
    let vary_blocks = vary_prevents_caching(&up_headers);
    let too_large = actual_size > state.max_cacheable_size;

    let cached_sha256: Option<String> = if !vary_blocks && !too_large {
        let final_key = vary_cache_key(&cache_key, &incoming, &up_headers);
        match state
            .store
            .insert_from_disk(
                &final_key,
                sha256.clone(),
                actual_size,
                &temp_path,
                &up_headers,
            )
            .await
        {
            Ok(()) => {
                debug!(key = %final_key, bytes = actual_size, "stored in cache");
                // Also index at the base key for Vary-dimension discovery.
                if final_key != cache_key {
                    state
                        .store
                        .insert_blob_ref(&cache_key, sha256.clone(), actual_size, &up_headers)
                        .await;
                }
                Some(sha256)
            }
            Err(e) => {
                warn!(error = %e, key = %final_key, "cache insert failed");
                None
            }
        }
    } else {
        if vary_blocks {
            debug!(key = %cache_key, "Vary header prevents caching");
        } else {
            warn!(
                key   = %cache_key,
                bytes = actual_size,
                max   = state.max_cacheable_size,
                "body exceeds --max-cacheable-size; not caching"
            );
        }
        None
    };

    // ------------------------------------------------------------------
    // Range upgrade: client wanted a range; we fetched the full body.
    // Slice now from disk.
    // ------------------------------------------------------------------
    if has_range
        && let Some(ref range_val) = client_range
        && let Some((start, end)) = parse_range_header(range_val, actual_size)
    {
        let serve_path = blob_path_on_disk(&blob_root, cached_sha256.as_deref())
            .unwrap_or_else(|| temp_path.clone());
        let slice = read_slice_from_path(&serve_path, start, end).await?;
        if cached_sha256.is_none() {
            let _ = tokio::fs::remove_file(&temp_path).await;
        }
        let mut resp = build_206_response(&up_headers, slice, start, end, actual_size);
        resp.headers_mut().insert(
            HeaderName::from_static("x-cache-key"),
            HeaderValue::from_str(&cache_key).unwrap_or_else(|_| HeaderValue::from_static("?")),
        );
        return Ok(resp);
    }
    // Unparseable range or no range → fall through to plain 200.

    // ------------------------------------------------------------------
    // Full 200 response — stream from blob (if cached) or temp file.
    // ------------------------------------------------------------------
    let body = if let Some(ref sha256) = cached_sha256 {
        // Temp file was renamed to the blob path; stream from there.
        let file = state.store.open_blob(sha256).await?;
        Body::from_stream(ReaderStream::new(file))
    } else {
        // Open temp file, then unlink it.  On Linux the fd stays valid after
        // the directory entry is removed, so the client still receives the body.
        let file = tokio::fs::File::open(&temp_path)
            .await
            .with_context(|| format!("reopening temp file {}", temp_path.display()))?;
        let _ = tokio::fs::remove_file(&temp_path).await;
        Body::from_stream(ReaderStream::new(file))
    };

    let mut builder = Response::builder().status(StatusCode::OK);
    let hdrs = builder.headers_mut().unwrap();
    for (name, value) in &up_headers {
        if !is_hop_by_hop(name.as_str()) {
            hdrs.append(name, value.clone());
        }
    }
    hdrs.insert(CONTENT_LENGTH, HeaderValue::from(actual_size));
    set_cache_headers(hdrs, "MISS", &cache_key);
    add_via(hdrs);

    Ok(builder.body(body).unwrap())
}

// ---------------------------------------------------------------------------
// Streaming helpers
// ---------------------------------------------------------------------------

/// Stream the response body to a temp file while computing SHA-256.
/// Returns (temp_path, sha256_hex, size_bytes).
async fn stream_to_temp(
    blob_root: &Path,
    mut resp: reqwest::Response,
) -> Result<(PathBuf, String, u64)> {
    let temp_path = make_temp_path(blob_root);
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .with_context(|| format!("creating temp file {}", temp_path.display()))?;
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    while let Some(chunk) = resp.chunk().await.context("reading upstream chunk")? {
        hasher.update(&chunk);
        total += chunk.len() as u64;
        file.write_all(&chunk)
            .await
            .context("writing chunk to temp file")?;
    }
    file.flush().await.context("flushing temp file")?;
    drop(file);
    let sha256 = hex::encode(hasher.finalize());
    Ok((temp_path, sha256, total))
}

/// Return the on-disk path for a cached sha256, or None if uncached.
fn blob_path_on_disk(blob_root: &Path, sha256: Option<&str>) -> Option<PathBuf> {
    sha256.map(|s| blob_root.join(&s[..2]).join(s))
}

/// Read bytes `[start, end)` from a file on disk.
async fn read_slice_from_path(path: &Path, start: u64, end: u64) -> Result<Bytes> {
    let mut f = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening file for slice {}", path.display()))?;
    f.seek(std::io::SeekFrom::Start(start)).await?;
    let len = (end - start) as usize;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).await.context("reading slice")?;
    Ok(Bytes::from(buf))
}

// ---------------------------------------------------------------------------
// Serving from cache
// ---------------------------------------------------------------------------

/// Serve full body from cache as streaming `200 OK`.
async fn serve_full_from_cache(
    entry: &CacheEntry,
    up304: &HeaderMap,
    cache_key: &str,
    state: &AppState,
) -> Result<Response> {
    let mut builder = Response::builder().status(StatusCode::OK);
    let hdrs = builder.headers_mut().unwrap();

    apply_cached_headers(hdrs, &entry.headers, up304);
    hdrs.insert(CONTENT_LENGTH, HeaderValue::from(entry.blob_size));
    set_cache_headers(hdrs, "HIT", cache_key);
    add_via(hdrs);

    let body = streaming_body_for_entry(entry, state).await?;
    Ok(builder.body(body).unwrap())
}

/// Serve a byte range from cache as `206 Partial Content`.
async fn serve_range_from_cache(
    entry: &CacheEntry,
    client_range: &Option<HeaderValue>,
    up304: &HeaderMap,
    cache_key: &str,
    state: &AppState,
) -> Result<Response> {
    let total = entry.blob_size;
    let Some(range_val) = client_range else {
        return serve_full_from_cache(entry, up304, cache_key, state).await;
    };

    let Some((start, end)) = parse_range_header(range_val, total) else {
        return Ok(Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header("content-range", format!("bytes */{total}"))
            .body(Body::empty())
            .unwrap());
    };

    let slice = read_slice(entry, start, end, state).await?;
    let mut resp = build_206_response(&entry.headers, slice, start, end, total);

    // Overlay fresh 304 headers (updated ETag, Cache-Control, …).
    for (name, value) in up304 {
        if !is_hop_by_hop(name.as_str()) {
            resp.headers_mut().insert(name, value.clone());
        }
    }
    set_cache_headers(resp.headers_mut(), "HIT", cache_key);
    add_via(resp.headers_mut());
    Ok(resp)
}

/// Read bytes [start, end) from a cache entry's blob.
async fn read_slice(entry: &CacheEntry, start: u64, end: u64, state: &AppState) -> Result<Bytes> {
    let path = {
        let br = state.store.blob_root();
        let s = &entry.blob_sha256;
        br.join(&s[..2]).join(s)
    };
    read_slice_from_path(&path, start, end).await
}

/// Produce a streaming `Body` for the full blob of a cache entry.
async fn streaming_body_for_entry(entry: &CacheEntry, state: &AppState) -> Result<Body> {
    let file = state.store.open_blob(&entry.blob_sha256).await?;
    Ok(Body::from_stream(ReaderStream::new(file)))
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn build_206_response(
    base_headers: &HeaderMap,
    slice: Bytes,
    start: u64,
    end: u64,
    total: u64,
) -> Response {
    let last_byte = end - 1; // Content-Range uses inclusive end
    let mut builder = Response::builder().status(StatusCode::PARTIAL_CONTENT);
    let hdrs = builder.headers_mut().unwrap();

    for (name, value) in base_headers {
        if !is_hop_by_hop(name.as_str()) && name != CONTENT_LENGTH {
            hdrs.append(name, value.clone());
        }
    }
    hdrs.insert(
        CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes {start}-{last_byte}/{total}")).unwrap(),
    );
    hdrs.insert(CONTENT_LENGTH, HeaderValue::from(slice.len()));
    hdrs.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    builder.body(Body::from(slice)).unwrap()
}

fn build_passthrough_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: Bytes,
    cache_key: &str,
) -> Response {
    let mut builder = Response::builder().status(status);
    let dst = builder.headers_mut().unwrap();
    for (name, value) in headers {
        if !is_hop_by_hop(name.as_str()) {
            dst.append(name, value.clone());
        }
    }
    set_cache_headers(dst, "MISS", cache_key);
    add_via(dst);
    builder.body(Body::from(body)).unwrap()
}

// ---------------------------------------------------------------------------
// Header helpers
// ---------------------------------------------------------------------------

/// Write stored response headers onto `dst`, then overlay end-to-end headers
/// from the upstream 304 response (RFC 7234 §4.3.4 header merging).
fn apply_cached_headers(dst: &mut HeaderMap, stored: &HeaderMap, fresh_304: &HeaderMap) {
    for (name, value) in stored {
        dst.append(name, value.clone());
    }
    for (name, value) in fresh_304 {
        if !is_hop_by_hop(name.as_str()) {
            dst.insert(name, value.clone());
        }
    }
}

fn set_cache_headers(dst: &mut HeaderMap, x_cache: &str, cache_key: &str) {
    dst.insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_str(x_cache).unwrap_or_else(|_| HeaderValue::from_static("?")),
    );
    dst.insert(
        HeaderName::from_static("x-cache-key"),
        HeaderValue::from_str(cache_key).unwrap_or_else(|_| HeaderValue::from_static("?")),
    );
}
