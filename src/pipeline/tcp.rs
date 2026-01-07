use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};

use crate::config::ForwardSpec;
use crate::pipeline::{ShutdownRx, copy_bidirectional};

/// Spawn a direct TCP proxy task that forwards bytes between host clients and a remote target.
///
/// The spec must provide `listen` for the local bind address and `target` for the remote endpoint.
pub fn spawn(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { tcp_proxy_loop(spec, shutdown).await })
}

#[instrument(skip_all, fields(listen = spec.listen.as_deref().unwrap_or_default(), target = spec.target.as_deref().unwrap_or_default()))]
async fn tcp_proxy_loop(spec: ForwardSpec, mut shutdown: ShutdownRx) -> Result<()> {
    let listen_addr = spec
        .listen
        .as_ref()
        .context("listen address missing for tcp proxy")?;
    let target = spec
        .target
        .clone()
        .context("tcp proxy requires target address")?;

    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", listen_addr))?;
    info!(%listen_addr, %target, "tcp proxy listening");

    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    info!(%listen_addr, "shutdown received; stopping tcp proxy");
                    break;
                }
            }
            accept_res = listener.accept() => {
                let (client, peer) = accept_res?;
                let target = target.clone();
                tokio::spawn(async move {
                    if let Err(err) = bridge_tcp(client, target).await {
                        warn!(peer = %peer, error = %err, "tcp proxy session failed");
                    }
                });
            }
        }
    }

    Ok(())
}

/// Dial the upstream target and forward bytes in both directions until either side closes.
async fn bridge_tcp(mut client: TcpStream, target: String) -> Result<()> {
    client.set_nodelay(true).ok();
    let mut upstream = TcpStream::connect(&target)
        .await
        .with_context(|| format!("tcp proxy failed to connect to {}", target))?;
    upstream.set_nodelay(true).ok();
    copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}
