# Validator Merging

When Relaycache has a cached entry **and** the client sends its own conditional
headers, both sets of validators must be forwarded to the origin so that either
can trigger a `304`. The origin's `304` response then tells us whose cache was
current, allowing Relaycache to return the right status to the client.

## ETag (`If-None-Match`)

`If-None-Match` accepts a comma-separated list of ETags (RFC 7232 §3.2):

```
If-None-Match: "client-etag", "proxy-etag"
```

The origin responds `304` if **either** matches, and echoes the winning ETag
in its `304` response headers.

### Merge rules

| Client has `If-None-Match` | Proxy has cached ETag | Action |
|---|---|---|
| No | No | nothing |
| Yes | No | forward client's header unchanged |
| No | Yes | inject proxy's ETag |
| Yes, same value | Yes, same | forward unchanged (already identical) |
| Yes, `"A"` | Yes, `"B"` | merge: `"A", "B"` |

### Determining who matched

After a `304`, Relaycache checks the `ETag` returned by the origin:

```
Returned ETag in client's If-None-Match list?
  Yes → client's cache is current → return 304 to client
  No  → proxy's ETag matched     → return 200 with cached body
```

If the origin returns no `ETag` in the `304`, fall through to the date check.

## Last-Modified (`If-Modified-Since`)

`If-Modified-Since` can only carry **one date**. Relaycache sends
`max(client_date, proxy_date)` — the newer of the two.

### Why max?

Let:
- **C** = client's `If-Modified-Since` date
- **P** = proxy's cached `Last-Modified` date
- **U** = actual last-modified date on the origin (unknown to us)

The origin returns `304` when `U ≤ forwarded_date`.

#### Case C > P (client is newer than proxy cache)

The client has a version we don't have cached.

| Send | Origin says | Meaning | Correct? |
|------|-------------|---------|----------|
| C (max) | 304 if U ≤ C | Client is current | ✅ |
| C (max) | 200 if U > C | Both stale; update cache | ✅ |
| P (min) | 304 if U ≤ P | Both current (P < C so U ≤ P ≤ C) | ✅ but rare |
| P (min) | 200 if P < U ≤ C | Wasteful: we'd download a body the client already has | ❌ wasted bandwidth |

**Verdict: send C.**

#### Case C < P (proxy is newer — the common case)

The proxy has a version the client doesn't have yet.

| Send | Origin says | Meaning | Correct? |
|------|-------------|---------|----------|
| P (max) | 304 if U ≤ P | Proxy is current; serve cached body as 200 to client | ✅ |
| P (max) | 200 if U > P | Both stale; update cache | ✅ |
| C (min) | 304 if U ≤ C | Client is current — but C < P so this is a contradiction (the proxy can't have cached a body newer than the origin's current version) | impossible |
| C (min) | 200 if C < U ≤ P | Wasteful: we'd download a body we already have in cache | ❌ wasted bandwidth |

**Verdict: send P.**

#### Summary: always send max(C, P)

Sending the newer date is strictly better in the asymmetric cases and neutral
in the symmetric case (C == P). `max(C, P)` minimises unnecessary `200`
responses.

### Determining who matched (date fallback)

When no `ETag` is present in the `304`, compare the origin's `Last-Modified`
(from the `304` headers) against the client's `If-Modified-Since`:

```
origin Last-Modified ≤ client If-Modified-Since?
  Yes → client's cache is current → return 304
  No  → proxy's date matched      → return 200 with cached body
```

When no usable validators exist at all, Relaycache conservatively assumes the
proxy matched and returns `200` with the cached body. This is always correct
(the client gets valid content) at the cost of one unnecessary body transfer.
