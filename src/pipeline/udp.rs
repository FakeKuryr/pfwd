use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::config::ForwardSpec;
use crate::pipeline::ShutdownRx;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Lightweight session that holds the remote-facing UDP socket and the JoinHandle in charge of
/// sending responses back to the originating client.
struct UdpSession {
    remote: Arc<UdpSocket>,
    last_seen: Instant,
    pump_handle: JoinHandle<()>,
}

pub fn spawn(spec: ForwardSpec, shutdown: ShutdownRx) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { udp_proxy_loop(spec, shutdown).await })
}

async fn udp_proxy_loop(spec: ForwardSpec, mut shutdown: ShutdownRx) -> Result<()> {
    let listen_addr = spec
        .udp_listen
        .as_ref()
        .context("udp proxy requires udp_listen address")?;
    let target_addr = spec
        .udp_target
        .as_ref()
        .context("udp proxy requires udp_target address")?
        .clone();
    let idle_timeout = spec.udp_idle_timeout();

    let client_socket = Arc::new(
        UdpSocket::bind(listen_addr)
            .await
            .with_context(|| format!("failed to bind udp listener {}", listen_addr))?,
    );
    info!(%listen_addr, %target_addr, idle_secs = idle_timeout.as_secs(), "udp proxy listening");

    let mut sessions: HashMap<SocketAddr, UdpSession> = HashMap::new();
    let mut cleanup = interval(CLEANUP_INTERVAL);
    let mut buf = vec![0u8; 65_507];

    loop {
        tokio::select! {
            biased;
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    info!(%listen_addr, "shutdown received; stopping udp proxy");
                    break;
                }
            }
            _ = cleanup.tick() => {
                prune_sessions(&mut sessions, idle_timeout);
            }
            recv = client_socket.recv_from(&mut buf) => {
                let (len, client_addr) = recv?;
                let mut drop_session = false;
                {
                    let session = match sessions.get_mut(&client_addr) {
                        Some(existing) => existing,
                        None => {
                            let session = create_session(
                                client_addr,
                                target_addr.clone(),
                                client_socket.clone(),
                                shutdown.clone(),
                            )
                            .await?;
                            sessions.insert(client_addr, session);
                            sessions.get_mut(&client_addr).expect("session just inserted")
                        }
                    };
                    session.last_seen = Instant::now();
                    if let Err(err) = session.remote.send(&buf[..len]).await {
                        warn!(client = %client_addr, error = %err, "failed to send udp datagram upstream");
                        drop_session = true;
                    }
                }
                if drop_session {
                    if let Some(session) = sessions.remove(&client_addr) {
                        session.pump_handle.abort();
                    }
                }
            }
        }
    }

    drain_sessions(sessions);
    Ok(())
}

/// Remove idle UDP sessions and abort their response pump tasks so resources are reclaimed.
fn prune_sessions(sessions: &mut HashMap<SocketAddr, UdpSession>, idle_timeout: Duration) {
    let now = Instant::now();
    sessions.retain(|client, session| {
        let idle = now.duration_since(session.last_seen);
        if idle > idle_timeout {
            debug!(client = %client, idle_secs = idle.as_secs(), "dropping idle udp session");
            session.pump_handle.abort();
            false
        } else {
            true
        }
    });
}

/// Abort any remaining per-client tasks when the UDP proxy loop is exiting.
fn drain_sessions(mut sessions: HashMap<SocketAddr, UdpSession>) {
    for (_, session) in sessions.drain() {
        session.pump_handle.abort();
    }
}

/// Create a new per-client relay socket and launch a task that copies remote responses back to the
/// original client address.
async fn create_session(
    client_addr: SocketAddr,
    target_addr: String,
    client_socket: Arc<UdpSocket>,
    shutdown: ShutdownRx,
) -> Result<UdpSession> {
    let remote_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .context("failed to bind udp relay socket")?,
    );
    remote_socket
        .connect(&target_addr)
        .await
        .with_context(|| format!("failed to connect udp target {}", target_addr))?;

    let remote_reader =
        spawn_remote_pump(remote_socket.clone(), client_socket, client_addr, shutdown);

    Ok(UdpSession {
        remote: remote_socket,
        last_seen: Instant::now(),
        pump_handle: remote_reader,
    })
}

/// Background loop that takes datagrams arriving from the remote target and forwards them back to
/// the originating client. It terminates when the session is idle or shutdown is triggered.
fn spawn_remote_pump(
    remote_socket: Arc<UdpSocket>,
    client_socket: Arc<UdpSocket>,
    client_addr: SocketAddr,
    mut shutdown: ShutdownRx,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65_507];
        loop {
            tokio::select! {
                biased;
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                recv = remote_socket.recv(&mut buf) => {
                    match recv {
                        Ok(len) => {
                            if let Err(err) = client_socket.send_to(&buf[..len], client_addr).await {
                                warn!(client = %client_addr, error = %err, "failed to forward udp response");
                                break;
                            }
                        }
                        Err(err) => {
                            warn!(client = %client_addr, error = %err, "udp remote recv failed");
                            break;
                        }
                    }
                }
            }
        }
    })
}
