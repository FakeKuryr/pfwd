use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use users::{get_group_by_name, get_user_by_name};

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    /// Optional path to a TOML configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Override log level (e.g. info, debug, trace).
    #[arg(long)]
    pub log_level: Option<String>,

    /// Inline forward specifications. Each entry is a comma-separated key=value list.
    ///
    /// Keys: listen, namespace, setns_path, uds, target, mode, owner, backlog, label.
    ///
    /// Example (host proxy):
    /// --forward listen=0.0.0.0:2222,uds=/run/qdhcp/ssh.sock
    ///
    /// Example (namespace endpoint):
    /// --forward namespace=qdhcp-1234,uds=/run/qdhcp/ssh.sock,target=192.168.31.201:22
    #[arg(long = "forward", value_name = "key=value")]
    pub inline_forwards: Vec<ForwardInline>,
}

#[derive(Debug, Clone)]
pub struct ForwardInline(pub ForwardSpec);

impl FromStr for ForwardInline {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut map = HashMap::new();
        for kv in s.split(',') {
            let (key, value) = kv
                .split_once('=')
                .context("forward entries must be key=value pairs separated by commas")?;
            map.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }

        let mut spec = ForwardSpec::default();
        if let Some(listen) = map.remove("listen") {
            spec.listen = Some(listen);
        }
        if let Some(namespace) = map.remove("namespace") {
            spec.namespace = Some(namespace);
        }
        if let Some(path) = map.remove("setns_path") {
            spec.setns_path = Some(PathBuf::from(path));
        }
        if let Some(uds) = map.remove("uds") {
            spec.uds = Some(PathBuf::from(uds));
        }
        if let Some(target) = map.remove("target") {
            spec.target = Some(target);
        }
        if let Some(mode) = map.remove("mode") {
            spec.mode = Some(parse_mode(&mode)?);
        }
        if let Some(owner) = map.remove("owner") {
            spec.owner = Some(owner.parse()?);
        }
        if let Some(backlog) = map.remove("backlog") {
            spec.backlog = Some(backlog.parse()?);
        }
        if let Some(label) = map.remove("label") {
            spec.label = Some(label);
        }

        if !map.is_empty() {
            bail!(
                "unknown forward keys: {}",
                map.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        }

        Ok(ForwardInline(spec))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct FileConfig {
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub forward: Vec<ForwardSpec>,
}

#[serde_as]
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Defaults {
    #[serde(default)]
    pub uds_dir: Option<PathBuf>,
    #[serde(default)]
    pub mode: Option<u32>,
    #[serde(default)]
    pub owner: Option<Owner>,
    #[serde(default)]
    pub backlog: Option<u32>,
}

#[serde_as]
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ForwardSpec {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub setns_path: Option<PathBuf>,
    #[serde(default)]
    pub uds: Option<PathBuf>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub mode: Option<u32>,
    #[serde(default)]
    pub owner: Option<Owner>,
    #[serde(default)]
    pub backlog: Option<u32>,
}

impl ForwardSpec {
    pub fn apply_defaults(&mut self, defaults: &Defaults) {
        if self.mode.is_none() {
            self.mode = defaults.mode;
        }
        if self.owner.is_none() {
            self.owner = defaults.owner.clone();
        }
        if self.backlog.is_none() {
            self.backlog = defaults.backlog;
        }
        if self.uds.is_none()
            && let (Some(dir), Some(label)) = (defaults.uds_dir.as_ref(), self.label.as_ref())
        {
            self.uds = Some(dir.join(format!("{label}.sock")));
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.uds.is_none() {
            bail!("missing uds path (set `uds` or provide defaults.uds_dir + label)");
        }
        if self.listen.is_none() && self.namespace.is_none() && self.setns_path.is_none() {
            bail!(
                "forward spec must define at least one of `listen`, `namespace`, or `setns_path`"
            );
        }
        if self.requires_namespace_endpoint() && self.target.is_none() {
            bail!("namespace endpoint requires `target` to be set");
        }
        Ok(())
    }

    pub fn requires_namespace_endpoint(&self) -> bool {
        self.target.is_some() && (self.namespace.is_some() || self.setns_path.is_some())
    }

    pub fn requires_host_proxy(&self) -> bool {
        self.listen.is_some()
    }

    pub fn uds_path(&self) -> &Path {
        self.uds.as_ref().expect("validated")
    }
}

#[serde_as]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Owner {
    #[serde_as(as = "DisplayFromStr")]
    pub uid: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub gid: u32,
}

impl FromStr for Owner {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (user, group) = s
            .split_once(':')
            .context("owner must be in user:group format")?;

        let uid = resolve_user(user)?;
        let gid = resolve_group(group)?;

        Ok(Self { uid, gid })
    }
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.uid, self.gid)
    }
}

fn resolve_user(input: &str) -> Result<u32> {
    if let Ok(id) = input.parse::<u32>() {
        return Ok(id);
    }
    let user = get_user_by_name(input).context("user not found")?;
    Ok(user.uid())
}

fn resolve_group(input: &str) -> Result<u32> {
    if let Ok(id) = input.parse::<u32>() {
        return Ok(id);
    }
    let group = get_group_by_name(input).context("group not found")?;
    Ok(group.gid())
}

fn parse_mode(value: &str) -> Result<u32> {
    if let Some(rest) = value.strip_prefix("0o") {
        return u32::from_str_radix(rest, 8).context("invalid octal mode");
    }
    if value.len() > 1
        && let Some(rest) = value.strip_prefix('0')
    {
        return u32::from_str_radix(rest, 8).context("invalid octal mode");
    }
    value.parse::<u32>().context("invalid mode")
}

pub fn load_config(cli: &Cli) -> Result<(Defaults, Vec<ForwardSpec>)> {
    let FileConfig { defaults, forward } = if let Some(path) = cli.config.as_ref() {
        let data = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        toml::from_str::<FileConfig>(&data)
            .with_context(|| format!("invalid TOML in {}", path.display()))?
    } else {
        FileConfig::default()
    };

    let mut forwards = forward;
    forwards.extend(cli.inline_forwards.iter().map(|f| f.0.clone()));

    for spec in forwards.iter_mut() {
        spec.apply_defaults(&defaults);
        spec.validate()?;
    }

    Ok((defaults, forwards))
}
