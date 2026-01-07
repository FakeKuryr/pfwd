use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::{TcpStream, UnixStream};
use tokio::runtime::Builder;
use tokio::task::{JoinHandle, spawn_blocking};
use tracing::{info, warn};

use crate::config::ForwardSpec;
use crate::netns;
use crate::pipeline::{ShutdownRx, copy_bidirectional};
use crate::uds::{BoundUnixListener, bind_listener};

const DEFAULT_BACKLOG: u32 = 64;

/// Enters the requested network namespace, binds the Unix Domain Socket, and forwards each accepted
/// UDS stream into the target TCP service inside the namespace.
pub fn spawn(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    spawn_blocking(move || {
        netns::maybe_enter(&spec)?;
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build namespace runtime")?;
        rt.block_on(namespace_loop(spec, shutdown))
    })
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

/// For each accepted UDS stream, open a TCP connection to the namespace-local target and stream
/// bytes until EOF.
async fn bridge_unix_to_tcp(mut unix_stream: UnixStream, target: String) -> Result<()> {
    let mut tcp = TcpStream::connect(&target)
        .await
        .with_context(|| format!("connect failed for target {}", target))?;
    tcp.set_nodelay(true).ok();
    copy_bidirectional(&mut unix_stream, &mut tcp).await?;
    Ok(())
}
