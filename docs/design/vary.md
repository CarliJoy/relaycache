# Vary Header Handling

The `Vary` response header tells caches which request headers affect the
response content. Relaycache uses it to produce per-variant cache keys.

## Cache key construction

The base cache key is `METHOD:path?query`. When the origin responds with a
`Vary` header, Relaycache extends the key with the values of the named request
headers:

```
Base key:    GET:/v2/library/nginx/manifests/latest
Vary: Accept
Request Accept: application/vnd.oci.image.manifest.v1+json

Final key:   GET:/v2/library/nginx/manifests/latest[accept=application/vnd.oci.image.manifest.v1+json]
```

Different `Accept` values produce different cache entries, each with their
own blob on disk.

## Special cases

### `Vary: *`

The response must not be stored. `Vary: *` means "every request is unique"
— the origin is opting out of caching entirely. Relaycache respects this and
passes the response through without storing it.

### `Vary: Authorization`

**The response is never cached.**

When the origin varies on `Authorization`, the response body is
user-personalised — different users get different content for the same URL.
Caching this would require either:

- Storing one body per credential (the cache fills with per-user copies)
- Using a shared body (wrong: user A gets user B's response)

Neither is acceptable. Relaycache treats `Vary: Authorization` as equivalent
to `Vary: *`.

This applies even when `Authorization` appears alongside other headers:
`Vary: Accept, Authorization` → not cached.

:::{note} Why not key by Authorization?
One might think: hash the token and include it in the cache key, so each
user gets their own entry. The problems are:

1. **Token rotation**: when tokens expire and refresh, old entries become
   orphans that waste space but never get hits.
2. **Design principle violation**: the cache exists to deduplicate bytes
   for **shared** content. Per-user content is not shared.
3. **Unnecessary complexity**: the origin already checks auth on every
   request. Per-credential caching adds no security benefit.
:::

### `Vary: Accept-Encoding`

Handled normally. Different encodings produce separate cache entries:

```
GET:/file[accept-encoding=gzip]     → blob A (compressed)
GET:/file[accept-encoding=identity] → blob B (uncompressed)
```

## Key lookup — two-phase approach

There is a bootstrapping problem: to compute the Vary-extended key, we need
to know which headers the origin varies on — but we only learn that from a
cached entry's stored headers.

Relaycache solves this with a two-phase lookup:

```
1. Look up base_key in moka
   → If found: read Vary from stored headers
              compute refined_key = vary_cache_key(base_key, request, entry.headers)
              if refined_key ≠ base_key: look up refined_key in moka
   → If not found: proceed with base_key (cache miss)

2. Use the result of phase 2 (or phase 1 if no refinement needed)
```

On the first ever request for a URL, there is no cached entry so the base
key is used. The origin's response includes `Vary`, which is stored in the
entry. On subsequent requests, the Vary dimensions are available and the
refined key is used.

## Authorization is never in the key

Even when `Authorization` appears in `Vary`, it is filtered out of the key
computation. The entry is marked uncacheable before the key is even computed.
There is no path by which auth credentials end up in a cache key or stored
in the database.
