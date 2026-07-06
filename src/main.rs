mod checkpoint;
mod config;
mod metrics;
mod storage;
mod streams;
mod websocket;

use clap::{Parser, Subcommand};
use clickhouse::Client;
use mimalloc::MiMalloc;
use std::{fs, net::SocketAddr, path::PathBuf, process::ExitCode, str::FromStr};
use tracing::{Level, debug, error};

use crate::{
    config::{IndexerConfig, load_config},
    metrics::Metrics,
    streams::{
        node_fills::{NodeFills, NodeFillsSinks},
        replica_cmds::{ReplicaCmds, ReplicaCmdsSinks},
    },
    websocket::WsServer,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "config/default.toml")]
    config: PathBuf,

    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    ParseReplicaCmds {
        #[arg(short, long)]
        path: PathBuf,
    },
    ParseNodeFills {
        #[arg(short, long)]
        path: PathBuf,
    },
}

async fn run_indexer(
    args: Args,
    config: IndexerConfig,
    ch: Client,
    metrics: &Metrics,
    ws: Option<WsServer>,
) -> anyhow::Result<()> {
    fs::create_dir_all(&config.checkpoints_dir)?;

    match args.command {
        Some(Commands::ParseReplicaCmds { path }) => {
            let sinks = ReplicaCmdsSinks::new(&ch, ws);
            streams::run::<ReplicaCmds>(&config, sinks, metrics, Some(path)).await
        }
        Some(Commands::ParseNodeFills { path }) => {
            let sinks = NodeFillsSinks::new(&ch, ws);
            streams::run::<NodeFills>(&config, sinks, metrics, Some(path)).await
        }
        None => {
            let replica_sinks = ReplicaCmdsSinks::new(&ch, ws.clone());
            let fills_sinks = NodeFillsSinks::new(&ch, ws);
            tokio::try_join!(
                streams::run::<ReplicaCmds>(&config, replica_sinks, metrics, None),
                streams::run::<NodeFills>(&config, fills_sinks, metrics, None),
            )?;
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    debug!("using config: {:?}", args.config);

    let metrics = Metrics::new();
    let config = load_config(args.config.to_path_buf()).unwrap();

    if config.metrics.enabled {
        let listen_addr = match SocketAddr::from_str(config.metrics.addr.as_str()) {
            Ok(addr) => addr,
            Err(e) => {
                error!(error = %e, "Invalid metrics listen address");
                return ExitCode::FAILURE;
            }
        };

        if let Err(e) = metrics.install_prometheus(listen_addr) {
            error!(error = %e, "Failed to start metrics server");
            return ExitCode::FAILURE;
        }
    }

    let ws = if config.websocket.enabled {
        let listen_addr = match SocketAddr::from_str(config.websocket.addr.as_str()) {
            Ok(addr) => addr,
            Err(e) => {
                error!(error = %e, "Invalid websocket listen address");
                return ExitCode::FAILURE;
            }
        };

        let server = WsServer::new();
        let ws = server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.run(listen_addr).await {
                error!(error = %e, "Websocket server failed");
            }
        });
        Some(ws)
    } else {
        None
    };

    let ch = Client::default()
        .with_url(&config.storage.url)
        .with_user(&config.storage.user)
        .with_password(&config.storage.password)
        .with_database(&config.storage.database)
        .with_option("input_format_binary_read_json_as_string", "1")
        .with_option("output_format_binary_write_json_as_string", "1");

    match run_indexer(args, config, ch, &metrics, ws).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(err = %e, "Indexer failed");
            ExitCode::FAILURE
        }
    }
}
