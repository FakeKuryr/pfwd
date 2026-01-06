mod config;
mod forward;
mod netns;
mod uds;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use tracing_subscriber::EnvFilter;

use crate::config::{Cli, load_config};

#[tokio::main]
async fn main() -> Result<()> {
    maybe_print_long_help();
    let cli = Cli::parse();
    init_tracing(cli.log_level.as_deref());
    let (_defaults, specs) = load_config(&cli)?;
    if specs.is_empty() {
        tracing::warn!("no forward entries configured");
        return Ok(());
    }
    forward::run(specs).await?;
    Ok(())
}

fn init_tracing(level: Option<&str>) {
    let filter = level
        .map(EnvFilter::new)
        .unwrap_or_else(EnvFilter::from_default_env);
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn maybe_print_long_help() {
    if std::env::args_os().any(|arg| arg == "-h") {
        let mut cmd = Cli::command();
        cmd.print_long_help().expect("failed to print help");
        println!();
        std::process::exit(0);
    }
}
