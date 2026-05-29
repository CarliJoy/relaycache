use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "relaycache",
    about = "Always-revalidating HTTP proxy with content-addressed disk cache"
)]
pub struct Config {
    /// Upstream base URL (e.g. https://registry.example.com)
    #[arg(env = "UPSTREAM")]
    pub upstream: String,

    /// TCP address to listen on (ignored when --unix-socket is set)
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8080")]
    pub bind: String,

    /// Unix domain socket path.  When set, overrides --bind.
    #[arg(long, env = "UNIX_SOCKET")]
    pub unix_socket: Option<PathBuf>,

    /// Directory for blob files and proxy.db.
    /// Defaults to $XDG_CACHE_HOME/relaycache/<upstream-name>
    /// (falls back to ~/.cache when XDG_CACHE_HOME is unset).
    #[arg(long, env = "CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Maximum number of in-memory index entries (LRU eviction by moka).
    #[arg(long, env = "CACHE_MAX_ENTRIES", default_value_t = 100_000)]
    pub cache_max_entries: u64,

    /// Bodies larger than this are not cached and are forwarded as-is.
    /// Accepts human-readable sizes: 512MiB, 1GiB, 200MB.
    #[arg(long, env = "MAX_CACHEABLE_SIZE", default_value = "512MiB")]
    pub max_cacheable_size: String,

    /// How long a cache entry lives without being accessed before eviction.
    /// Accepts human-readable durations: 24h, 7days, 30min.
    #[arg(long, env = "ENTRY_TTL", default_value = "24h")]
    pub entry_ttl: String,

    /// How often the background eviction job runs.
    /// Accepts human-readable durations: 1h, 30min.
    #[arg(long, env = "EVICTION_INTERVAL", default_value = "1h")]
    pub eviction_interval: String,
}

/// Parsed and validated configuration.
pub struct ParsedConfig {
    pub upstream: String,
    pub bind: String,
    pub unix_socket: Option<PathBuf>,
    pub cache_dir: PathBuf,
    pub cache_max_entries: u64,
    pub max_cacheable_size: u64,
    pub entry_ttl: Duration,
    pub eviction_interval: Duration,
}

impl Config {
    pub fn parse_and_validate(self) -> Result<ParsedConfig> {
        let max_cacheable_size = parse_size(&self.max_cacheable_size)
            .map_err(|e| anyhow::anyhow!("--max-cacheable-size: {e}"))?;

        let entry_ttl = humantime::parse_duration(&self.entry_ttl)
            .map_err(|e| anyhow::anyhow!("--entry-ttl: {e}"))?;

        let eviction_interval = humantime::parse_duration(&self.eviction_interval)
            .map_err(|e| anyhow::anyhow!("--eviction-interval: {e}"))?;

        let upstream = self.upstream.trim_end_matches('/').to_owned();

        let cache_dir = match self.cache_dir {
            Some(dir) => dir,
            None => {
                let xdg_cache = std::env::var_os("XDG_CACHE_HOME")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        let home = std::env::var_os("HOME")
                            .map(PathBuf::from)
                            .unwrap_or_else(|| PathBuf::from("."));
                        home.join(".cache")
                    });
                xdg_cache
                    .join("relaycache")
                    .join(upstream_fs_name(&upstream))
            }
        };

        Ok(ParsedConfig {
            upstream,
            bind: self.bind,
            unix_socket: self.unix_socket,
            cache_dir,
            cache_max_entries: self.cache_max_entries,
            max_cacheable_size,
            entry_ttl,
            eviction_interval,
        })
    }
}

/// Convert an upstream URL into a filesystem-safe directory name.
///
/// Strips the scheme, then replaces every character that is not
/// alphanumeric, `-`, or `.` with `_`, and trims leading/trailing `_`.
///
/// Examples:
/// - `https://registry.example.com`      → `registry.example.com`
/// - `https://registry.example.com:5000` → `registry.example.com_5000`
/// - `https://registry.example.com/v2`   → `registry.example.com_v2`
fn upstream_fs_name(upstream: &str) -> String {
    let without_scheme = upstream
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    without_scheme
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned()
}

/// Parse a human-readable size string into bytes.
/// Supports: 512MiB, 1GiB, 200MB, 1TB, 100KB, 4096 (bare bytes).
fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    // Try bare integer first.
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    // Split into number + suffix.
    let split = s
        .find(|c: char| c.is_alphabetic())
        .ok_or_else(|| anyhow::anyhow!("no unit found in size '{s}'"))?;
    let num: f64 = s[..split]
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number in size '{s}'"))?;
    let unit = s[split..].trim().to_ascii_uppercase();
    let multiplier: u64 = match unit.as_str() {
        "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_048_576,
        "GIB" => 1_073_741_824,
        "TIB" => 1_099_511_627_776,
        _ => bail!("unknown size unit '{unit}' in '{s}'"),
    };
    Ok((num * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_mib() {
        assert_eq!(parse_size("512MiB").unwrap(), 512 * 1_048_576);
    }

    #[test]
    fn parse_size_gb() {
        assert_eq!(parse_size("1GB").unwrap(), 1_000_000_000);
    }

    #[test]
    fn parse_size_bare() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
    }

    #[test]
    fn parse_size_unknown_unit() {
        assert!(parse_size("100XX").is_err());
    }

    #[test]
    fn upstream_fs_name_simple() {
        assert_eq!(
            upstream_fs_name("https://registry.example.com"),
            "registry.example.com"
        );
    }

    #[test]
    fn upstream_fs_name_with_port() {
        assert_eq!(
            upstream_fs_name("https://registry.example.com:5000"),
            "registry.example.com_5000"
        );
    }

    #[test]
    fn upstream_fs_name_with_path() {
        assert_eq!(
            upstream_fs_name("https://registry.example.com/v2"),
            "registry.example.com_v2"
        );
    }
}
