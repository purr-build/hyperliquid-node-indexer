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
        hip3_oracle_updates::Hip3OracleUpdates, misc_events::MiscEvents, node_fills::NodeFills,
        node_twap_statuses::NodeTwapStatuses, replica_cmds::ReplicaCmds,
        system_and_core_writer_actions::SystemAndCoreWriterActions,
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
    #[command(name = "parse-replica-cmds")]
    ReplicaCmds {
        #[arg(short, long)]
        path: PathBuf,
    },
    #[command(name = "parse-node-fills")]
    NodeFills {
        #[arg(short, long)]
        path: PathBuf,
    },
    #[command(name = "parse-misc-events")]
    MiscEvents {
        #[arg(short, long)]
        path: PathBuf,
    },
    #[command(name = "parse-node-twap-statuses")]
    NodeTwapStatuses {
        #[arg(short, long)]
        path: PathBuf,
    },
    #[command(name = "parse-system-and-core-writer-actions")]
    SystemAndCoreWriterActions {
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
        Some(Commands::ReplicaCmds { path }) => {
            let stream = ReplicaCmds::new(&ch, ws);
            streams::run(&config, stream, metrics, Some(path)).await
        }
        Some(Commands::NodeFills { path }) => {
            let stream = NodeFills::new(&ch, ws);
            streams::run(&config, stream, metrics, Some(path)).await
        }
        Some(Commands::MiscEvents { path }) => {
            let stream = MiscEvents::new(&ch);
            streams::run(&config, stream, metrics, Some(path)).await
        }
        Some(Commands::NodeTwapStatuses { path }) => {
            let stream = NodeTwapStatuses::new(&ch);
            streams::run(&config, stream, metrics, Some(path)).await
        }
        Some(Commands::SystemAndCoreWriterActions { path }) => {
            let stream = SystemAndCoreWriterActions::new(&ch);
            streams::run(&config, stream, metrics, Some(path)).await
        }
        None => {
            let replica_cmds = ReplicaCmds::new(&ch, ws.clone());
            let node_fills = NodeFills::new(&ch, ws.clone());
            let hip3_oracle_updates = Hip3OracleUpdates::new(&ch, ws);
            let misc_events = MiscEvents::new(&ch);
            let node_twap_statuses = NodeTwapStatuses::new(&ch);
            let system_and_core_writer_actions = SystemAndCoreWriterActions::new(&ch);

            tokio::try_join!(
                streams::run(&config, replica_cmds, metrics, None),
                streams::run(&config, node_fills, metrics, None),
                streams::run(&config, hip3_oracle_updates, metrics, None),
                streams::run(&config, misc_events, metrics, None),
                streams::run(&config, node_twap_statuses, metrics, None),
                streams::run(&config, system_and_core_writer_actions, metrics, None),
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
