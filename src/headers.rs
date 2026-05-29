//! HTTP header utilities: forwarding, Vary key construction, validator merging.

use axum::http::{
    HeaderMap, HeaderName, HeaderValue,
    header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, VARY},
};
use tracing::{debug, warn};

use crate::store::CacheEntry;

// ---------------------------------------------------------------------------
// Hop-by-hop headers (RFC 7230 §6.1)
// ---------------------------------------------------------------------------

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name.to_ascii_lowercase().as_str())
}

/// Copy client headers to forwarded set, stripping hop-by-hop.
pub fn forward_headers(src: &HeaderMap) -> HeaderMap {
    let mut dst = HeaderMap::new();
    for (name, value) in src {
        if !is_hop_by_hop(name.as_str()) {
            dst.append(name, value.clone());
        }
    }
    dst
}

// ---------------------------------------------------------------------------
// Via header (RFC 7230 §5.7.1)
// ---------------------------------------------------------------------------

/// The `Via` value this proxy adds to both forwarded requests and responses.
const VIA_VALUE: &str = "1.1 relaycache";

/// Append `1.1 relaycache` to the `Via` header (or create it).
pub fn add_via(headers: &mut HeaderMap) {
    // If a Via header already exists, append to the list.
    let existing = headers
        .get("via")
        .and_then(|v| v.to_str().ok())
        .map(|s| format!("{s}, {VIA_VALUE}"))
        .unwrap_or_else(|| VIA_VALUE.to_owned());
    headers.insert(
        HeaderName::from_static("via"),
        HeaderValue::from_str(&existing).unwrap_or_else(|_| HeaderValue::from_static(VIA_VALUE)),
    );
}

// ---------------------------------------------------------------------------
// Vary-aware cache key
// ---------------------------------------------------------------------------

/// Compute the base cache key: `METHOD:path?query`.
///
/// Auth headers are never part of the key — every credential is forwarded
/// to the upstream; the cache only deduplicates body bytes.
pub fn base_cache_key(method: &axum::http::Method, uri: &axum::http::Uri) -> String {
    let pq = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    format!("{method}:{pq}")
}

/// Extend a base key with Vary dimensions from the upstream response headers.
///
/// Returns the original `base` string unchanged when:
/// - no `Vary` header is present
/// - `Vary: *` (callers check for this separately)
/// - all Vary fields are `Authorization` (which is always excluded)
///
/// `Authorization` is explicitly never included in the Vary key.
/// See docs/design/vary.md for the full rationale.
pub fn vary_cache_key(base: &str, req_headers: &HeaderMap, resp_headers: &HeaderMap) -> String {
    let vary = match resp_headers.get(VARY) {
        Some(v) => v,
        None => return base.to_owned(),
    };
    let vary_str = vary.to_str().unwrap_or("");

    // Vary: * → sentinel; callers must not cache this response.
    if vary_str.trim() == "*" {
        return format!("{base}:vary=*");
    }

    let mut parts: Vec<String> = vary_str
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|name| !name.eq_ignore_ascii_case("authorization"))
        .map(|name| {
            let val = req_headers
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            format!("{name}={val}")
        })
        .collect();

    if parts.is_empty() {
        return base.to_owned();
    }
    parts.sort(); // deterministic regardless of Vary field order
    format!("{base}[{}]", parts.join(","))
}

/// Returns true if the upstream response's Vary header makes caching unsafe.
pub fn vary_prevents_caching(resp_headers: &HeaderMap) -> bool {
    let Some(vary) = resp_headers.get(VARY) else {
        return false;
    };
    let s = vary.to_str().unwrap_or("");
    s.trim() == "*"
        || s.split(',')
            .any(|f| f.trim().eq_ignore_ascii_case("authorization"))
}

// ---------------------------------------------------------------------------
// Validator merging
// ---------------------------------------------------------------------------

/// Merge `If-None-Match` from the client and the proxy cache.
///
/// `If-None-Match` accepts a comma-separated ETag list (RFC 7232 §3.2).
/// We send both so the upstream can match against either and tell us which
/// one matched via the `ETag` in its 304 response.
///
/// | Client   | Proxy    | Action                        |
/// |----------|----------|-------------------------------|
/// | none     | none     | nothing                       |
/// | some     | none     | already forwarded; nothing    |
/// | none     | some     | inject proxy ETag             |
/// | "A"      | "A"      | already forwarded; nothing    |
/// | "A"      | "B"      | merge → `"A", "B"`            |
pub fn merge_etag_validators(incoming: &HeaderMap, entry: &CacheEntry, fwd: &mut HeaderMap) {
    match (incoming.get(IF_NONE_MATCH), entry.etag.as_ref()) {
        (None, Some(p)) => {
            fwd.insert(IF_NONE_MATCH, p.clone());
            debug!("If-None-Match: injected proxy ETag");
        }
        (Some(c), Some(p)) if c != p => {
            let merged = format!("{}, {}", c.to_str().unwrap_or(""), p.to_str().unwrap_or(""));
            debug!(merged = %merged, "If-None-Match: merged client + proxy ETags");
            if let Ok(v) = HeaderValue::from_str(&merged) {
                fwd.insert(IF_NONE_MATCH, v);
            }
        }
        _ => {} // (None,None), equal ETags, or (Some,None) — already correct
    }
}

/// Merge `If-Modified-Since` (client) and `Last-Modified` (proxy cache),
/// forwarding `max(C, P)` — the newer date.
///
/// Sending the newer date is strictly better in the asymmetric cases:
/// - C > P: sending C avoids a wasteful 200 for content the client already has
/// - C < P: sending P avoids a wasteful 200 for content the proxy has cached
/// - C = P: trivially equivalent
///
/// See docs/design/validator-merging.md for the full case analysis.
pub fn merge_ims_validators(incoming: &HeaderMap, entry: &CacheEntry, fwd: &mut HeaderMap) {
    match (
        incoming.get(IF_MODIFIED_SINCE),
        entry.last_modified.as_ref(),
    ) {
        (None, Some(p)) => {
            fwd.insert(IF_MODIFIED_SINCE, p.clone());
            debug!("If-Modified-Since: injected proxy Last-Modified");
        }
        (Some(c), Some(p)) => match (parse_http_date(c), parse_http_date(p)) {
            (Some(c_secs), Some(p_secs)) => {
                let winner = if c_secs >= p_secs { c } else { p };
                debug!(
                    chosen = if c_secs >= p_secs { "client" } else { "proxy" },
                    "If-Modified-Since: forwarding max(client, proxy)"
                );
                fwd.insert(IF_MODIFIED_SINCE, winner.clone());
            }
            _ => {
                warn!("If-Modified-Since: could not parse date(s); using proxy date");
                fwd.insert(IF_MODIFIED_SINCE, p.clone());
            }
        },
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// 304 decision: did the client's validator match?
// ---------------------------------------------------------------------------

/// After receiving a 304, decide whether to return 304 (client is current)
/// or 200/206 (proxy cache matched, client needs the body).
///
/// ETag path (preferred): the upstream echoes the winning ETag in the 304
/// per RFC 7232 §6.3.2.
///
/// Date fallback: if no ETag in the 304, compare Last-Modified against the
/// client's If-Modified-Since.
///
/// Default (no usable validators) → false → serve our cached body.
pub fn did_client_validator_match(client: &HeaderMap, up304: &HeaderMap) -> bool {
    // ETag path
    if let Some(returned) = up304.get(ETAG) {
        return client
            .get(IF_NONE_MATCH)
            .map(|inm| etag_list_contains(inm, returned))
            .unwrap_or(false);
    }
    // Date fallback
    if let (Some(c_ims), Some(lm)) = (client.get(IF_MODIFIED_SINCE), up304.get(LAST_MODIFIED))
        && let (Some(c), Some(l)) = (parse_http_date(c_ims), parse_http_date(lm))
    {
        return l <= c;
    }
    false
}

// ---------------------------------------------------------------------------
// ETag utilities
// ---------------------------------------------------------------------------

/// True if `candidate` appears in the comma-separated `list`.
/// Uses weak comparison (RFC 7232 §2.3): `W/"x"` == `"x"`.
pub fn etag_list_contains(list: &HeaderValue, candidate: &HeaderValue) -> bool {
    let cand = normalize_etag(candidate.to_str().unwrap_or(""));
    list.to_str()
        .unwrap_or("")
        .split(',')
        .map(|t| normalize_etag(t.trim()))
        .any(|t| t == cand)
}

/// Strip weak prefix and surrounding quotes: `W/"foo"` → `foo`.
pub fn normalize_etag(s: &str) -> &str {
    s.strip_prefix("W/").unwrap_or(s).trim_matches('"')
}

// ---------------------------------------------------------------------------
// HTTP date parsing
// ---------------------------------------------------------------------------

/// Parse an HTTP-date (RFC 7231 §7.1.1.1) into Unix seconds for ordering.
pub fn parse_http_date(value: &HeaderValue) -> Option<i64> {
    let s = value.to_str().ok()?;
    parse_imf_fixdate(s).or_else(|| parse_rfc850_date(s))
}

fn parse_imf_fixdate(s: &str) -> Option<i64> {
    let s = s.get(5..)?; // skip "Mon, "
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() < 4 {
        return None;
    }
    let (day, mon, year): (i64, i64, i64) =
        (p[0].parse().ok()?, month_num(p[1])?, p[2].parse().ok()?);
    let t: Vec<&str> = p[3].split(':').collect();
    if t.len() < 3 {
        return None;
    }
    let (h, m, s): (i64, i64, i64) = (t[0].parse().ok()?, t[1].parse().ok()?, t[2].parse().ok()?);
    Some(unix_ts(year, mon, day, h, m, s))
}

fn parse_rfc850_date(s: &str) -> Option<i64> {
    let s = s.find(',').and_then(|i| s.get(i + 2..))?;
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() < 3 {
        return None;
    }
    let d: Vec<&str> = p[0].split('-').collect();
    if d.len() < 3 {
        return None;
    }
    let (day, mon): (i64, i64) = (d[0].parse().ok()?, month_num(d[1])?);
    let yy: i64 = d[2].parse().ok()?;
    let year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
    let t: Vec<&str> = p[1].split(':').collect();
    if t.len() < 3 {
        return None;
    }
    let (h, m, s): (i64, i64, i64) = (t[0].parse().ok()?, t[1].parse().ok()?, t[2].parse().ok()?);
    Some(unix_ts(year, mon, day, h, m, s))
}

fn month_num(m: &str) -> Option<i64> {
    Some(match m {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}

fn unix_ts(year: i64, month: i64, day: i64, h: i64, m: i64, s: i64) -> i64 {
    // Leap years that fell between 1970 and year-1 (absolute proleptic Gregorian).
    // 1969/4 - 1969/100 + 1969/400 = 492 - 19 + 4 = 477.
    let leap = (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400 - 477;
    let mut days = (year - 1970) * 365 + leap;
    const MD: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in MD.iter().take((month - 1) as usize) {
        days += m;
    }
    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if is_leap && month > 2 {
        days += 1;
    }
    days += day - 1;
    days * 86400 + h * 3600 + m * 60 + s
}

// ---------------------------------------------------------------------------
// Range header parsing
// ---------------------------------------------------------------------------

/// Parse `Range: bytes=start-end` into `(start, end)` where end is exclusive.
/// Returns `None` for multi-range, unsatisfiable, or malformed headers.
pub fn parse_range_header(value: &HeaderValue, total: u64) -> Option<(u64, u64)> {
    let s = value.to_str().ok()?.strip_prefix("bytes=")?;
    if s.contains(',') {
        return None; // multi-range not supported
    }
    let (start_s, end_s) = s.split_once('-')?;
    let (start, end) = if start_s.is_empty() {
        let n: u64 = end_s.parse().ok()?;
        (total.saturating_sub(n), total)
    } else {
        let start: u64 = start_s.parse().ok()?;
        let end = if end_s.is_empty() {
            total
        } else {
            end_s.parse::<u64>().ok()?.saturating_add(1).min(total)
        };
        (start, end)
    };
    if start >= total || start >= end {
        return None;
    }
    Some((start, end))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn hv(s: &str) -> HeaderValue {
        HeaderValue::from_str(s).unwrap()
    }

    // --- Vary -----------------------------------------------------------------

    #[test]
    fn vary_key_no_vary() {
        let resp = HeaderMap::new();
        assert_eq!(
            vary_cache_key("GET:/foo", &HeaderMap::new(), &resp),
            "GET:/foo"
        );
    }

    #[test]
    fn vary_key_accept() {
        let mut resp = HeaderMap::new();
        resp.insert(VARY, hv("Accept"));
        let mut req = HeaderMap::new();
        req.insert(axum::http::header::ACCEPT, hv("application/json"));
        let key = vary_cache_key("GET:/foo", &req, &resp);
        assert!(key.contains("accept=application/json"), "key={key}");
    }

    #[test]
    fn vary_key_ignores_authorization() {
        let mut resp = HeaderMap::new();
        resp.insert(VARY, hv("Authorization, Accept"));
        let mut req = HeaderMap::new();
        req.insert(axum::http::header::AUTHORIZATION, hv("Bearer secret"));
        req.insert(axum::http::header::ACCEPT, hv("application/json"));
        let key = vary_cache_key("GET:/foo", &req, &resp);
        assert!(
            !key.to_ascii_lowercase().contains("secret"),
            "credential leaked: {key}"
        );
        assert!(key.contains("accept=application/json"), "key={key}");
    }

    #[test]
    fn vary_prevents_caching_star() {
        let mut resp = HeaderMap::new();
        resp.insert(VARY, hv("*"));
        assert!(vary_prevents_caching(&resp));
    }

    #[test]
    fn vary_prevents_caching_authorization() {
        let mut resp = HeaderMap::new();
        resp.insert(VARY, hv("Accept, Authorization"));
        assert!(vary_prevents_caching(&resp));
    }

    #[test]
    fn vary_does_not_prevent_normal() {
        let mut resp = HeaderMap::new();
        resp.insert(VARY, hv("Accept"));
        assert!(!vary_prevents_caching(&resp));
    }

    // --- ETag -----------------------------------------------------------------

    #[test]
    fn etag_exact() {
        assert!(etag_list_contains(&hv(r#""abc""#), &hv(r#""abc""#)));
    }

    #[test]
    fn etag_weak_equals_strong() {
        assert!(etag_list_contains(&hv(r#"W/"abc""#), &hv(r#""abc""#)));
    }

    #[test]
    fn etag_list_second() {
        assert!(etag_list_contains(&hv(r#""a", "b", "c""#), &hv(r#""b""#)));
    }

    #[test]
    fn etag_no_match() {
        assert!(!etag_list_contains(&hv(r#""a", "b""#), &hv(r#""c""#)));
    }

    // --- Dates ----------------------------------------------------------------

    #[test]
    fn date_epoch() {
        assert_eq!(
            parse_http_date(&hv("Thu, 01 Jan 1970 00:00:00 GMT")),
            Some(0)
        );
    }

    #[test]
    fn date_y2k() {
        assert_eq!(
            parse_http_date(&hv("Sat, 01 Jan 2000 00:00:00 GMT")),
            Some(946684800)
        );
    }

    #[test]
    fn date_ordering() {
        let a = parse_http_date(&hv("Mon, 01 Jan 2024 00:00:00 GMT")).unwrap();
        let b = parse_http_date(&hv("Tue, 02 Jan 2024 00:00:00 GMT")).unwrap();
        assert!(a < b);
    }

    #[test]
    fn date_rfc850() {
        assert_eq!(
            parse_http_date(&hv("Monday, 02-Jan-06 15:04:05 GMT")),
            Some(1136214245)
        );
    }

    // --- Range ----------------------------------------------------------------

    #[test]
    fn range_normal() {
        assert_eq!(parse_range_header(&hv("bytes=0-499"), 1000), Some((0, 500)));
    }

    #[test]
    fn range_open_end() {
        assert_eq!(
            parse_range_header(&hv("bytes=500-"), 1000),
            Some((500, 1000))
        );
    }

    #[test]
    fn range_suffix() {
        assert_eq!(
            parse_range_header(&hv("bytes=-200"), 1000),
            Some((800, 1000))
        );
    }

    #[test]
    fn range_clamp() {
        assert_eq!(
            parse_range_header(&hv("bytes=0-9999"), 1000),
            Some((0, 1000))
        );
    }

    #[test]
    fn range_unsatisfiable() {
        assert_eq!(parse_range_header(&hv("bytes=2000-2999"), 1000), None);
    }

    #[test]
    fn range_multi() {
        assert_eq!(parse_range_header(&hv("bytes=0-9,20-29"), 1000), None);
    }

    // --- Via ------------------------------------------------------------------

    #[test]
    fn via_added() {
        let mut headers = HeaderMap::new();
        add_via(&mut headers);
        assert_eq!(
            headers.get("via").unwrap().to_str().unwrap(),
            "1.1 relaycache"
        );
    }

    #[test]
    fn via_appended() {
        let mut headers = HeaderMap::new();
        headers.insert("via", hv("1.1 upstream-proxy"));
        add_via(&mut headers);
        let via = headers.get("via").unwrap().to_str().unwrap();
        assert!(via.contains("upstream-proxy"), "via={via}");
        assert!(via.contains("relaycache"), "via={via}");
    }

    // --- did_client_validator_match -------------------------------------------

    #[test]
    fn client_etag_matches() {
        let mut c = HeaderMap::new();
        c.insert(IF_NONE_MATCH, hv(r#""v1""#));
        let mut u = HeaderMap::new();
        u.insert(ETAG, hv(r#""v1""#));
        assert!(did_client_validator_match(&c, &u));
    }

    #[test]
    fn proxy_etag_only() {
        let mut c = HeaderMap::new();
        c.insert(IF_NONE_MATCH, hv(r#""v1""#));
        let mut u = HeaderMap::new();
        u.insert(ETAG, hv(r#""v2""#));
        assert!(!did_client_validator_match(&c, &u));
    }

    #[test]
    fn date_fallback_client_current() {
        let mut c = HeaderMap::new();
        c.insert(IF_MODIFIED_SINCE, hv("Tue, 02 Jan 2024 00:00:00 GMT"));
        let mut u = HeaderMap::new();
        u.insert(LAST_MODIFIED, hv("Mon, 01 Jan 2024 00:00:00 GMT"));
        assert!(did_client_validator_match(&c, &u));
    }
}
