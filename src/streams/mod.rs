pub mod hip3_oracle_updates;
pub mod misc_events;
pub mod node_fills;
pub mod node_twap_statuses;
pub mod replica_cmds;
pub mod system_and_core_writer_actions;

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use clickhouse::{Row, inserter::Inserter};
use futures::stream::StreamExt;
use tailer::{Event, Position, Tailer, collector::FilesCollector};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::{
    checkpoint::{load, save},
    config::IndexerConfig,
    metrics::Metrics,
    storage::{Decimal, decimal_from_str},
};

const CHECKPOINT_INTERVAL: Duration = Duration::from_millis(5000);
const LOG_INTERVAL: Duration = Duration::from_secs(5);

#[allow(async_fn_in_trait)]
pub trait Stream {
    /// Stream name; used for metric labels, logs, and the checkpoint file.
    const NAME: &'static str;
    /// Subdirectory of `data_dir` the node writes this stream to.
    const SOURCE_DIR: &'static str;

    type Rows: Send + 'static;

    fn parse(event: Event) -> anyhow::Result<Self::Rows>;

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()>;

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()>;
}

pub async fn run<S: Stream>(
    config: &IndexerConfig,
    mut stream: S,
    metrics: &Metrics,
    explicit_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let source_path = PathBuf::from(&config.data_dir).join(S::SOURCE_DIR);
    let checkpoint_file = format!("{}.json", S::NAME);
    let (tailer, checkpoint_path) = build_tailer(
        source_path,
        &config.checkpoints_dir,
        &checkpoint_file,
        explicit_path,
        S::NAME,
    )?;

    let events = ReceiverStream::new(tailer.run()?)
        .map(|ev| {
            let start = Instant::now();
            let pos = ev.position.clone();
            let parsed = S::parse(ev);
            metrics.record_parse_duration(S::NAME, start.elapsed().as_secs_f64());
            (pos, parsed)
        })
        .filter_map(|(pos, res)| async move { res.ok().map(|rows| (pos, rows)) });

    let mut checkpoint_ticker = tokio::time::interval(CHECKPOINT_INTERVAL);
    tokio::pin!(events);

    let mut pending: Option<Position> = None;
    let mut last_log = Instant::now();

    loop {
        tokio::select! {
            item = events.next() => match item {
                Some((pos, rows)) => {
                    stream.write(&rows, metrics).await?;
                    stream.commit(metrics, false).await?;

                    if last_log.elapsed() >= LOG_INTERVAL {
                        debug!(
                            stream = S::NAME,
                            "progress: {}:{}",
                            pos.path.display(),
                            pos.line_number,
                        );
                        last_log = Instant::now();
                    }

                    pending = Some(pos);
                }
                None => break,
            },
            _ = checkpoint_ticker.tick() => {
                stream.commit(metrics, true).await?;

                if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
                    debug!("saving checkpoint {:?}", cp);
                    save(cp, p)?;
                }
            }
        }
    }

    stream.commit(metrics, true).await?;

    if let (Some(cp), Some(p)) = (&checkpoint_path, &pending) {
        save(cp, p)?;
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

            tailer.with_files(FilesCollector::single(path));

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

pub async fn commit_metered<T: Row>(
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

pub(crate) fn parse_datetime_nanos(value: &str) -> anyhow::Result<DateTime<Utc>> {
    Ok(NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")?.and_utc())
}

pub(crate) fn parse_millis(value: u64) -> anyhow::Result<DateTime<Utc>> {
    use chrono::TimeZone;
    let value = i64::try_from(value).map_err(|_| anyhow::anyhow!("millis timestamp overflow"))?;
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid millis timestamp"))
}

pub(crate) fn parse_decimal_field(value: &str, field: &str) -> anyhow::Result<Decimal> {
    decimal_from_str(value).map_err(|e| anyhow::anyhow!(format!("parse {field}: {e}")))
}

pub(crate) fn parse_optional_decimal_field(
    value: Option<&str>,
    field: &str,
) -> anyhow::Result<Option<Decimal>> {
    value
        .map(|value| parse_decimal_field(value, field))
        .transpose()
}
