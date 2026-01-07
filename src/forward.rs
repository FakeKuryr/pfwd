use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;

use crate::config::ForwardSpec;
use crate::pipeline::{self, ShutdownRx, host, namespace, tcp, udp};

pub async fn run(specs: Vec<ForwardSpec>) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = pipeline::shutdown_channel();
    let signal_handle = pipeline::spawn_shutdown_listener(shutdown_tx.clone());
    let mut tasks: FuturesUnordered<JoinHandle<Result<()>>> = FuturesUnordered::new();
    for spec in specs {
        enqueue_tasks(&mut tasks, spec, shutdown_rx.clone());
    }

    let mut first_err: Option<anyhow::Error> = None;
    while let Some(result) = tasks.next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = shutdown_tx.send(true);
                first_err.get_or_insert(err);
            }
            Err(join_err) => {
                let _ = shutdown_tx.send(true);
                first_err.get_or_insert(join_err.into());
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

fn enqueue_tasks(
    tasks: &mut FuturesUnordered<JoinHandle<Result<()>>>,
    spec: ForwardSpec,
    shutdown_rx: ShutdownRx,
) {
    if spec.requires_namespace_endpoint() {
        tasks.push(namespace::spawn(spec.clone(), shutdown_rx.clone()));
    }
    if spec.requires_host_uds_proxy() {
        tasks.push(host::spawn(spec.clone(), shutdown_rx.clone()));
    }
    if spec.requires_direct_tcp_proxy() {
        tasks.push(tcp::spawn(spec.clone(), shutdown_rx.clone()));
    }
    if spec.requires_udp_proxy() {
        tasks.push(udp::spawn(spec, shutdown_rx));
    }
}
