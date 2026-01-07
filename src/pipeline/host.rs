use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io;
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{info, instrument, warn};

use crate::config::ForwardSpec;
use crate::pipeline::{ShutdownRx, copy_bidirectional};

const UDS_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const UDS_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

/// Listens for TCP clients on the host and tunnels each session through the configured Unix
/// Domain Socket. This corresponds to the "host proxy" leg in the docs.
pub fn spawn(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { host_proxy_loop(spec, shutdown).await })
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

/// Establish a Unix stream to the namespace endpoint and ferry traffic between it and the original
/// TCP client.
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
