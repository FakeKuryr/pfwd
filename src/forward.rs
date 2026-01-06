use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io;
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::runtime::Builder;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::sleep;
use tracing::{info, instrument, warn};

use crate::config::ForwardSpec;
use crate::netns;
use crate::uds::{BoundUnixListener, bind_listener};

const DEFAULT_BACKLOG: u32 = 64;
const UDS_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const UDS_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

type ShutdownRx = watch::Receiver<bool>;

pub async fn run(specs: Vec<ForwardSpec>) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_handle = spawn_shutdown_listener(shutdown_tx.clone());
    let mut tasks = Vec::new();
    for spec in specs {
        if spec.requires_namespace_endpoint() {
            tasks.push(spawn_namespace_task(spec.clone(), shutdown_rx.clone()));
        }
        if spec.requires_host_proxy() {
            tasks.push(spawn_host_proxy(spec, shutdown_rx.clone()));
        }
    }

    let mut first_err: Option<anyhow::Error> = None;
    for handle in tasks {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = shutdown_tx.send(true);
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(join_err) => {
                let _ = shutdown_tx.send(true);
                if first_err.is_none() {
                    first_err = Some(join_err.into());
                }
            }
        }
    }

    signal_handle.abort();
    let _ = signal_handle.await;

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

fn spawn_namespace_task(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    spawn_blocking(move || {
        netns::maybe_enter(&spec)?;
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build namespace runtime")?;
        rt.block_on(namespace_loop(spec, shutdown))
    })
}

fn spawn_host_proxy(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { host_proxy_loop(spec, shutdown).await })
}

async fn namespace_loop(spec: ForwardSpec, shutdown: ShutdownRx) -> Result<()> {
    let spec = Arc::new(spec);
    let uds_path = spec.uds_path().to_path_buf();
    let backlog = spec.backlog.unwrap_or(DEFAULT_BACKLOG);
    let owner = spec.owner.clone();
    let mode = spec.mode;
    let guard = bind_listener(&uds_path, owner, mode)?;
    info!(
        label = spec.label.as_deref().unwrap_or("unnamed"),
        uds = %uds_path.display(),
        target = spec.target.as_deref().unwrap_or(""),
        backlog,
        "namespace endpoint listening"
    );

    namespace_accept_loop(guard, spec, shutdown).await
}

async fn namespace_accept_loop(
    guard: BoundUnixListener,
    spec: Arc<ForwardSpec>,
    mut shutdown: ShutdownRx,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    info!(label = spec.label.as_deref().unwrap_or("unnamed"), "shutdown received; stopping namespace endpoint");
                    break;
                }
            }
            accept_res = guard.accept() => {
                let (stream, _) = accept_res?;
                let target = spec
                    .target
                    .clone()
                    .expect("validated target missing unexpectedly");
                let spec_label = spec.label.clone();
                tokio::spawn(async move {
                    if let Err(err) = bridge_unix_to_tcp(stream, target).await {
                        warn!(label = spec_label.as_deref().unwrap_or("unnamed"), error = %err, "bridge failed");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn bridge_unix_to_tcp(mut unix_stream: UnixStream, target: String) -> Result<()> {
    let mut tcp = TcpStream::connect(&target)
        .await
        .with_context(|| format!("connect failed for target {}", target))?;
    tcp.set_nodelay(true).ok();
    copy_bidirectional(&mut unix_stream, &mut tcp).await?;
    Ok(())
}

#[instrument(skip_all, fields(listen = spec.listen.as_deref().unwrap_or_default()))]
async fn host_proxy_loop(spec: ForwardSpec, mut shutdown: ShutdownRx) -> Result<()> {
    let listen_addr = spec
        .listen
        .as_ref()
        .context("listen address missing for host proxy")?;
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", listen_addr))?;
    info!(%listen_addr, "host proxy listening");
    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    info!(%listen_addr, "shutdown received; stopping host proxy");
                    break;
                }
            }
            accept_res = listener.accept() => {
                let (tcp, peer) = accept_res?;
                let uds = spec.uds_path().to_path_buf();
                tokio::spawn(async move {
                    if let Err(err) = bridge_tcp_to_unix(tcp, uds).await {
                        warn!(peer = %peer, error = %err, "session failed");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn bridge_tcp_to_unix(mut tcp: TcpStream, uds: std::path::PathBuf) -> Result<()> {
    tcp.set_nodelay(true).ok();
    let mut delay = UDS_RETRY_INITIAL_DELAY;
    let mut attempts = 0u32;
    let mut unix = loop {
        match UnixStream::connect(&uds).await {
            Ok(stream) => {
                if attempts > 0 {
                    info!(uds = %uds.display(), attempts, "uds became available");
                }
                break stream;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                attempts += 1;
                warn!(
                    uds = %uds.display(),
                    attempts,
                    wait_ms = delay.as_millis() as u64,
                    "uds not found; backing off"
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2);
                if delay > UDS_RETRY_MAX_DELAY {
                    delay = UDS_RETRY_MAX_DELAY;
                }
            }
            Err(err) => return Err(err.into()),
        }
    };
    copy_bidirectional(&mut tcp, &mut unix).await?;
    Ok(())
}

async fn copy_bidirectional<A, B>(a: &mut A, b: &mut B) -> Result<()>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::io::copy_bidirectional(a, b).await?;
    Ok(())
}

fn spawn_shutdown_listener(shutdown: watch::Sender<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("ctrl_c received; initiating shutdown");
                let _ = shutdown.send(true);
            }
            Err(err) => {
                warn!(error = %err, "failed to listen for ctrl_c");
            }
        }
    })
}
