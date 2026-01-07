use std::path::PathBuf;

#[cfg(target_os = "linux")]
use anyhow::{Context, Result};
#[cfg(not(target_os = "linux"))]
use anyhow::{Result, bail};

use crate::config::ForwardSpec;

#[cfg(target_os = "linux")]
pub fn maybe_enter(spec: &ForwardSpec) -> Result<()> {
    use nix::sched::{CloneFlags, setns};
    use std::fs::File;

    let Some(path) = desired_netns_path(spec) else {
        return Ok(());
    };

    let file = File::open(&path)
        .with_context(|| format!("failed to open namespace file {}", path.display()))?;
    setns(&file, CloneFlags::CLONE_NEWNET)
        .with_context(|| format!("setns failed for {}", path.display()))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn maybe_enter(spec: &ForwardSpec) -> Result<()> {
    if spec.namespace.is_some() || spec.setns_path.is_some() {
        bail!("network namespaces are unsupported on this platform");
    }
    Ok(())
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn desired_netns_path(spec: &ForwardSpec) -> Option<PathBuf> {
    if let Some(path) = spec.setns_path.as_ref() {
        return Some(path.clone());
    }
    spec.namespace
        .as_ref()
        .map(|ns| PathBuf::from("/var/run/netns").join(ns))
}
