use std::fs;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use nix::libc;
use nix::sys::stat::{Mode, fchmod};
use nix::unistd::{Gid, Uid, fchown};
use tokio::net::unix::SocketAddr;
use tokio::net::{UnixListener, UnixStream};

use crate::config::Owner;

pub struct BoundUnixListener {
    path: PathBuf,
    listener: UnixListener,
}

impl BoundUnixListener {
    pub async fn accept(&self) -> std::io::Result<(UnixStream, SocketAddr)> {
        self.listener.accept().await
    }
}

impl Drop for BoundUnixListener {
    fn drop(&mut self) {
        match fs::remove_file(&self.path) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %err,
                    "failed to remove unix socket during drop"
                );
            }
        };
    }
}

pub fn bind_listener(
    path: &Path,
    owner: Option<Owner>,
    mode: Option<u32>,
) -> Result<BoundUnixListener> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create socket directory {}", parent.display()))?;
    }

    if path.exists() {
        match fs::metadata(path) {
            Ok(meta) if meta.file_type().is_socket() => {
                fs::remove_file(path)
                    .with_context(|| format!("failed to remove stale socket {}", path.display()))?;
            }
            Ok(_) => {
                bail!("{} exists and is not a unix socket", path.display());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("stat failed for {}", path.display()));
            }
        }
    }

    let std_listener = StdUnixListener::bind(path)
        .with_context(|| format!("unable to bind unix socket {}", path.display()))?;
    std_listener
        .set_nonblocking(true)
        .context("failed to set nonblocking mode for unix listener")?;

    if let Some(mode) = mode {
        let bits: libc::mode_t = mode
            .try_into()
            .context("mode must fit into platform mode_t")?;
        let mode = Mode::from_bits(bits).context("invalid mode bits")?;
        fchmod(std_listener.as_raw_fd(), mode)?;
    }
    if let Some(owner) = owner {
        fchown(
            std_listener.as_raw_fd(),
            Some(Uid::from_raw(owner.uid)),
            Some(Gid::from_raw(owner.gid)),
        )
        .with_context(|| format!("failed to chown {}", path.display()))?;
    }

    let listener = UnixListener::from_std(std_listener)?;
    Ok(BoundUnixListener {
        path: path.to_path_buf(),
        listener,
    })
}
