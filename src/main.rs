//! Relaycache — always-revalidating HTTP proxy with content-addressed disk cache.
//!
//! See README.md and docs/ for full documentation.

#![forbid(unsafe_code)]

mod config;
mod headers;
mod proxy;
mod store;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use clap::Parser;
use reqwest::Client;
use tracing::info;

use config::Config;
use proxy::{AppState, handle, health};
use store::{CacheStore, eviction_task};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "relaycache=info".into()),
        )
        .init();

    let cfg = Config::parse().parse_and_validate()?;

    info!(
        upstream          = %cfg.upstream,
        cache_dir         = ?cfg.cache_dir,
        max_cacheable     = cfg.max_cacheable_size,
        entry_ttl         = ?cfg.entry_ttl,
        eviction_interval = ?cfg.eviction_interval,
        "relaycache starting"
    );

    let (store, db_handle) = CacheStore::open(&cfg.cache_dir, cfg.cache_max_entries).await?;

    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .use_rustls_tls()
        .build()
        .context("building HTTP client")?;

    let state = AppState {
        upstream: Arc::new(cfg.upstream.clone()),
        client,
        store: store.clone(),
        max_cacheable_size: cfg.max_cacheable_size,
    };

    // Background eviction job.
    tokio::spawn(eviction_task(
        store.clone(),
        cfg.entry_ttl,
        cfg.eviction_interval,
        cfg.cache_dir.join("blobs").join("sha256"),
        cfg.cache_dir.join("proxy.db"),
    ));

    let app = Router::new()
        .route("/__relaycache/health", get(health))
        .fallback(handle)
        .with_state(state);

    // Graceful shutdown on Ctrl-C / SIGTERM.
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        info!("shutdown signal received");
    };

    if let Some(ref sock) = cfg.unix_socket {
        let _ = tokio::fs::remove_file(sock).await;
        let listener = tokio::net::UnixListener::bind(sock)
            .with_context(|| format!("binding unix socket {}", sock.display()))?;
        info!(socket = %sock.display(), "listening on unix socket");
        serve_unix(listener, app, shutdown).await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&cfg.bind)
            .await
            .with_context(|| format!("binding TCP {}", cfg.bind))?;
        info!(addr = %cfg.bind, "listening on TCP");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await?;
    }

    // Clean shutdown: signal DB writer to drain and checkpoint.
    info!("draining database writer…");
    store.shutdown();

    // Wait for the DB writer task to finish.
    let _ = tokio::time::timeout(Duration::from_secs(10), db_handle).await;

    info!("relaycache stopped");
    Ok(())
}

/// Serve an axum app over a Unix domain socket with graceful shutdown.
/// axum::serve only accepts TcpListener, so we drive hyper-util directly.
/// TowerToHyperService bridges axum's tower::Service impl to hyper's Service trait.
async fn serve_unix(
    listener: tokio::net::UnixListener,
    app: Router,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.context("accepting unix connection")?;
                let io = TokioIo::new(stream);
                let svc = TowerToHyperService::new(app.clone());
                tokio::spawn(async move {
                    Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, svc)
                        .await
                        .ok();
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}
