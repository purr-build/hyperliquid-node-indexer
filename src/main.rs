mod checkpoint;
mod config;
mod metrics;
mod parsers;
mod storage;
mod websocket;

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use clickhouse::{Client, Row, inserter::Inserter};
use futures::stream::StreamExt;
use mimalloc::MiMalloc;
use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    process::ExitCode,
    str::FromStr,
    time::{Duration, Instant},
};
use tailer::{Position, Tailer, collector::FilesCollector};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Level, debug, error, info};

use crate::{
    checkpoint::{load, save},
    config::{IndexerConfig, load_config},
    metrics::Metrics,
    parsers::{parse_node_fills, parse_replica_cmds},
    storage::{ActionRow, BlockRow, NodeFillRow, SignedActionBundleRow, new_inserter},
    websocket::{WsPublisher, WsServer},
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const CHECKPOINT_INTERVAL: Duration = Duration::from_millis(5000);
const LOG_INTERVAL: Duration = Duration::from_secs(5);

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

async fn commit_metered<T: Row>(
    writer: &mut Inserter<T>,
    table: &str,
    metrics: &Metrics,
    force: bool,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let quantities = if force {
        writer.force_commit().await?
    } else {
        writer.commit().await?
    };
    if quantities.rows > 0 {
        metrics.record_commit_duration(table, start.elapsed().as_secs_f64());
    }
    Ok(())
}

fn build_tailer(
    source_path: PathBuf,
    checkpoints_dir: &str,
    checkpoint_file: &str,
    explicit_path: Option<PathBuf>,
    stream: &str,
) -> anyhow::Result<(Tailer, Option<PathBuf>)> {
    let mut tailer = Tailer::new(source_path.clone());

    match explicit_path {
        Some(path) => {
            tailer.with_start_position(Position {
                line_number: 0,
                path: path.clone(),
                offset: 0,
            });

            let parent = path
                .parent()
                .with_context(|| format!("{} has no parent directory", path.display()))?;
            let files_collector = FilesCollector::new(parent.to_path_buf(), |a, b| a.cmp(b))?;
            tailer.with_files(files_collector);

            Ok((tailer, None))
        }
        None => {
            tailer.with_follow();

            let checkpoint_path = PathBuf::from(checkpoints_dir).join(checkpoint_file);
            if let Some(pos) = load(&checkpoint_path)? {
                info!(stream, "resuming from checkpoint at {pos:?}");
                tailer.with_start_position(pos);
            }

            let files_collector = FilesCollector::new(source_path, |a, b| a.cmp(b))?;
            tailer.with_files(files_collector);

            Ok((tailer, Some(checkpoint_path)))
        }
    }
}

struct ReplicaCmdsIndexer<'a> {
    tailer: Tailer,
    ch: Client,
    metrics: &'a Metrics,
    checkpoint_path: Option<PathBuf>,
    ws: Option<WsPublisher>,
}

impl<'a> ReplicaCmdsIndexer<'a> {
    pub fn new(
        config: &IndexerConfig,
        ch: Client,
        metrics: &'a Metrics,
        path: Option<PathBuf>,
        ws: Option<WsPublisher>,
    ) -> anyhow::Result<Self> {
        let source_path = PathBuf::from(&config.data_dir).join("replica_cmds");
        let (tailer, checkpoint_path) = build_tailer(
            source_path,
            &config.checkpoints_dir,
            "replica_cmds.json",
            path,
            "replica_cmds",
        )?;

        Ok(Self {
            tailer,
            ch,
            metrics,
            checkpoint_path,
            ws,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Self {
            tailer,
            ch,
            metrics,
            checkpoint_path,
            ws,
        } = self;

        let stream = ReceiverStream::new(tailer.run()?);
        let stream = stream
            .map(move |ev| {
                let start = Instant::now();
                let filename: u64 = ev
                    .file
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .parse()
                    .unwrap();
                let parsed = parse_replica_cmds(filename + ev.position.line_number as u64, ev.line);
                metrics.record_parse_duration("replica_cmds", start.elapsed().as_secs_f64());
                (ev.position, parsed)
            })
            .filter_map(|(pos, res)| async move { res.ok().map(|rows| (pos, rows)) });

        let mut blocks_writer: Inserter<BlockRow> = new_inserter(&ch, "blocks");
        let mut bundles_writer: Inserter<SignedActionBundleRow> =
            new_inserter(&ch, "signed_action_bundle");
        let mut actions_writer: Inserter<ActionRow> = new_inserter(&ch, "actions");

        let mut checkpoint_ticker = tokio::time::interval(CHECKPOINT_INTERVAL);
        tokio::pin!(stream);

        let mut pending: Option<Position> = None;
        let mut last_block_log = Instant::now();

        loop {
            tokio::select! {
                item = stream.next() => match item {
                    Some((pos, rows)) => {
                        let lag = (Utc::now() - rows.block.time).as_seconds_f64();
                        metrics.record_ingested("replica_cmds", "block");
                        metrics.record_lag("replica_cmds", lag);
                        blocks_writer.write(&rows.block).await?;

                        if let Some(ws) = &ws {
                            ws.publish_block(&rows.block);
                        }

                        for b in &rows.bundles {
                            metrics.record_ingested("replica_cmds", "bundle");
                            bundles_writer.write(b).await?;
                        }

                        for a in &rows.actions {
                            metrics.record_ingested("replica_cmds", "action");
                            actions_writer.write(a).await?;
                        }

                        commit_metered(&mut blocks_writer, "blocks", metrics, false).await?;
                        commit_metered(&mut bundles_writer, "signed_action_bundle", metrics, false)
                            .await?;
                        commit_metered(&mut actions_writer, "actions", metrics, false).await?;

                        pending = Some(pos);

                        if last_block_log.elapsed() >= LOG_INTERVAL {
                            debug!("latest parsed block {} at {}", rows.block.number, rows.block.time);
                            last_block_log = Instant::now();
                        }
                    }
                    None => break,
                },
                _ = checkpoint_ticker.tick() => {
                    commit_metered(&mut blocks_writer, "blocks", metrics, true).await?;
                    commit_metered(&mut bundles_writer, "signed_action_bundle", metrics, true).await?;
                    commit_metered(&mut actions_writer, "actions", metrics, true).await?;

                    if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
                        debug!("saving checkpoint {:?}", cp);
                        save(cp, p)?;
                    }
                }
            }
        }

        commit_metered(&mut blocks_writer, "blocks", metrics, true).await?;
        commit_metered(&mut bundles_writer, "signed_action_bundle", metrics, true).await?;
        commit_metered(&mut actions_writer, "actions", metrics, true).await?;

        if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
            save(cp, p)?;
        }

        Ok(())
    }
}

struct NodeFillsIndexer<'a> {
    tailer: Tailer,
    ch: Client,
    metrics: &'a Metrics,
    checkpoint_path: Option<PathBuf>,
    ws: Option<WsPublisher>,
}

impl<'a> NodeFillsIndexer<'a> {
    pub fn new(
        config: &IndexerConfig,
        ch: Client,
        metrics: &'a Metrics,
        path: Option<PathBuf>,
        ws: Option<WsPublisher>,
    ) -> anyhow::Result<Self> {
        let source_path = PathBuf::from(&config.data_dir).join("node_fills_streaming");
        let (tailer, checkpoint_path) = build_tailer(
            source_path,
            &config.checkpoints_dir,
            "node_fills.json",
            path,
            "node_fills",
        )?;

        Ok(Self {
            tailer,
            ch,
            metrics,
            checkpoint_path,
            ws,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Self {
            tailer,
            ch,
            metrics,
            checkpoint_path,
            ws,
        } = self;

        let stream = ReceiverStream::new(tailer.run()?);
        let stream = stream
            .map(move |ev| {
                let start = Instant::now();
                let parsed = parse_node_fills(ev.line);
                metrics.record_parse_duration("node_fills", start.elapsed().as_secs_f64());
                (ev.position, parsed)
            })
            .filter_map(|(pos, res)| async move { res.ok().map(|rows| (pos, rows)) });

        let mut fills_writer: Inserter<NodeFillRow> = new_inserter(&ch, "node_fills");

        let mut checkpoint_ticker = tokio::time::interval(CHECKPOINT_INTERVAL);
        tokio::pin!(stream);

        let mut pending: Option<Position> = None;
        let mut last_fill_log = Instant::now();

        loop {
            tokio::select! {
                item = stream.next() => match item {
                    Some((pos, rows)) => {
                        for row in &rows {
                            let lag = (Utc::now() - row.block_time).as_seconds_f64();
                            metrics.record_ingested("node_fills", "fill");
                            metrics.record_lag("node_fills", lag);
                            fills_writer.write(row).await?;
                        }

                        if let Some(ws) = &ws {
                            ws.publish_fills(&rows);
                        }

                        commit_metered(&mut fills_writer, "node_fills", metrics, false).await?;

                        pending = Some(pos);

                        if last_fill_log.elapsed() >= LOG_INTERVAL {
                            if let Some(row) = rows.last() {
                                debug!(
                                    "latest parsed node fill block {} at {}",
                                    row.block_number, row.block_time
                                );
                                last_fill_log = Instant::now();
                            }
                        }
                    }
                    None => break,
                },
                _ = checkpoint_ticker.tick() => {
                    commit_metered(&mut fills_writer, "node_fills", metrics, true).await?;

                    if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
                        debug!("saving checkpoint {:?}", cp);
                        save(cp, p)?;
                    }
                }
            }
        }

        commit_metered(&mut fills_writer, "node_fills", metrics, true).await?;

        if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
            save(cp, p)?;
        }

        Ok(())
    }
}

async fn run_indexer(
    args: Args,
    config: IndexerConfig,
    ch: Client,
    metrics: &Metrics,
    ws: Option<WsPublisher>,
) -> anyhow::Result<()> {
    fs::create_dir_all(&config.checkpoints_dir)?;

    match args.command {
        Some(Commands::ParseReplicaCmds { path }) => {
            ReplicaCmdsIndexer::new(&config, ch, metrics, Some(path), ws)?
                .run()
                .await
        }
        Some(Commands::ParseNodeFills { path }) => {
            NodeFillsIndexer::new(&config, ch, metrics, Some(path), ws)?
                .run()
                .await
        }
        None => {
            let replica_cmds =
                ReplicaCmdsIndexer::new(&config, ch.clone(), metrics, None, ws.clone())?;
            let node_fills = NodeFillsIndexer::new(&config, ch, metrics, None, ws)?;

            tokio::try_join!(replica_cmds.run(), node_fills.run())?;
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
        let publisher = server.publisher();
        tokio::spawn(async move {
            if let Err(e) = server.run(listen_addr).await {
                error!(error = %e, "Websocket server failed");
            }
        });
        Some(publisher)
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
