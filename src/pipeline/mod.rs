pub mod host;
pub mod namespace;
pub mod tcp;
pub mod udp;

use anyhow::Result;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub type ShutdownRx = watch::Receiver<bool>;
pub type ShutdownTx = watch::Sender<bool>;

pub fn shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    watch::channel(false)
}

pub fn spawn_shutdown_listener(shutdown: ShutdownTx) -> JoinHandle<()> {
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

pub async fn copy_bidirectional<A, B>(a: &mut A, b: &mut B) -> Result<()>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::io::copy_bidirectional(a, b).await?;
    Ok(())
}
