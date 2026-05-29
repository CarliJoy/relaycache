# Range Requests

HTTP `Range` requests (`Range: bytes=N-M`) allow clients to fetch a portion
of a response body. Relaycache handles these with a "full-fetch upgrade" strategy.

## Strategy: always cache the full body

Relaycache never caches partial responses. When a range is requested, Relaycache
fetches (or uses) the **full body** and slices it for the client. This means:

- The **first** range request for an uncached resource causes a full download
- **All subsequent** requests (ranged or not) are served from the full cached body

This trades first-request latency for dramatically better subsequent performance.
It is the same strategy used by production container registries (Harbor, Nexus).

## Case 1: Full body already cached

The most important case. Client sends `Range: bytes=N-M`, proxy has the full
body cached.

```
Client: GET /blob  Range: bytes=0-999
                   If-Range: "etag-client-has"   (optional)

Relaycache strips Range and If-Range before forwarding:
Relaycache→Origin: GET /blob  If-None-Match: "cached-etag"

Origin → 304 Not Modified
  Relaycache serves bytes [0, 1000) from cache as 206
  No body re-download. Auth was checked. ✅

Origin → 200 (ETag changed)
  Relaycache downloads new body, updates cache
  Serves bytes [0, 1000) from new body as 206 ✅
```

Why strip `Range`? Because we want a full-file conditional GET (`If-None-Match`)
so the origin can respond `304`. If we forwarded `Range`, the origin would respond
`206` (not `304`), losing the ability to reuse the cached body.

## Case 2: No cache, client sends Range

```
Client: GET /blob  Range: bytes=1000-1999

Relaycache has no cache entry. Upgrade to full fetch:
Relaycache→Origin: GET /blob   (no Range header)

Origin → 200 with full body
  Relaycache caches full body
  Serves bytes [1000, 2000) as 206 to client ✅

Origin → body > --max-cacheable-size
  Relaycache forwards the upstream 206 as-is (pass-through)
  Nothing cached ✅
```

## Case 3: Range beyond end of file

```
Client: GET /blob  Range: bytes=9999-99999
                   (file is 1000 bytes)

Relaycache: range is unsatisfiable → 416 Range Not Satisfiable
         Content-Range: bytes */1000
```

## Case 4: Multi-range (`bytes=0-9,20-29`)

Not supported. Relaycache falls through to a plain `200` response with the full
body. Multi-range responses require `multipart/byteranges` body assembly which
adds significant complexity for a rare use case.

## The `If-Range` header

`If-Range` is sent by clients that have a partial response and want to resume
or extend it: "give me this range, but only if the ETag still matches; if not,
give me the whole new file."

Relaycache strips `If-Range` when it has the full body cached (Case 1) because
Relaycache handles the conditional check itself via `If-None-Match`. The client's
`If-Range` is irrelevant — Relaycache has the full body regardless.

When there is no cache (Case 2), `If-Range` is also stripped because Relaycache
upgrades to a full unconditional fetch.

## 206 response construction

When serving a range from cache, Relaycache constructs the `206` response by:

1. Copying the stored response headers (Content-Type, ETag, Last-Modified, etc.)
2. Setting `Content-Range: bytes start-end/total` (inclusive end per RFC)
3. Setting `Content-Length` to the slice length
4. Setting `Accept-Ranges: bytes`
5. Overlaying any end-to-end headers from the origin's `304` (updated ETag, etc.)
6. Setting `X-Cache: HIT`

## Auth enforcement on range requests

Even when serving a range from cache, Relaycache **always contacts the origin first**.
The sequence for a cached range hit:

```
1. Client sends Range request
2. Relaycache forwards to origin with If-None-Match (no Range)
3. Origin checks auth → if 401/403, return that to client; do not serve range
4. Origin checks freshness → 304
5. Relaycache slices cached body → 206 to client
```

A user whose access is revoked gets `401` at step 3, never the cached bytes.
