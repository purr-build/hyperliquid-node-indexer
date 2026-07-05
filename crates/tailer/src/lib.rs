pub mod collector;

use std::fs::File;

use metrics::{Counter, counter};
use notify::event::ModifyKind;
use notify::{EventKind, RecursiveMode, Result, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::sync::mpsc::{self, Sender};
use tracing::{debug, error, info};

use crate::collector::FilesCollector;

struct Metrics {
    lines_read: Counter,
}

impl Metrics {
    fn new() -> Self {
        Self {
            lines_read: counter!(
                description: "Total lines processed by tailer",
                "tailer_lines_total",
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Position {
    pub path: PathBuf,
    pub line_number: usize,
    pub offset: usize,
}

struct Tail {
    file: File,
    accum: Vec<u8>,
    position: Position,
}

pub struct Event {
    pub file: PathBuf,
    pub position: Position,
    pub line: Vec<u8>,
}

pub struct Tailer {
    dir: PathBuf,
    files: Option<FilesCollector>,
    start_position: Option<Position>,

    follow: bool,

    line_tx: mpsc::Sender<Event>,
    line_rx: mpsc::Receiver<Event>,

    notify_tx: mpsc::Sender<notify::Result<notify::Event>>,
    notify_rx: mpsc::Receiver<notify::Result<notify::Event>>,

    metrics: Metrics,
}

fn read_and_emit(
    tail: &mut Tail,
    line_tx: &Sender<Event>,
    metrics: &Metrics,
) -> std::io::Result<bool> {
    const CHUNK: usize = 512 * 1024;
    let mut buf = [0u8; CHUNK];

    debug!(
        "start reading {:?} from offset {}",
        tail.position.path, tail.position.offset
    );

    loop {
        let n = tail.file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        tail.accum.extend_from_slice(&buf[..n]);

        let mut start = 0;
        while let Some(rel) = memchr::memchr(b'\n', &tail.accum[start..]) {
            let end = start + rel + 1; // include '\n'
            let line = tail.accum[start..end].to_vec();
            tail.position.offset += line.len();
            tail.position.line_number += 1;

            let event = Event {
                file: tail.position.path.clone(),
                position: tail.position.clone(),
                line,
            };

            metrics.lines_read.increment(1);

            if line_tx.blocking_send(event).is_err() {
                return Ok(false);
            }

            start = end;
        }

        tail.accum.drain(..start);
    }

    Ok(true)
}

fn canonical(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

impl Tailer {
    pub fn new(dir: PathBuf) -> Self {
        let (notify_tx, notify_rx) = mpsc::channel::<notify::Result<notify::Event>>(128);
        let (line_tx, line_rx) = mpsc::channel::<Event>(4096);

        Self {
            dir,
            files: None,
            line_tx,
            line_rx,
            notify_tx,
            notify_rx,
            start_position: None,
            follow: false,
            metrics: Metrics::new(),
        }
    }

    pub fn with_files(&mut self, files: FilesCollector) {
        self.files = Some(files);
    }

    pub fn with_start_position(&mut self, position: Position) {
        self.start_position = Some(position);
    }

    pub fn with_follow(&mut self) {
        self.follow = true;
    }

    pub fn run(self) -> Result<mpsc::Receiver<Event>> {
        let Tailer {
            dir,
            files,
            notify_tx,
            mut notify_rx,
            line_tx,
            line_rx,
            start_position,
            follow,
            metrics,
        } = self;

        let watcher = if follow {
            let mut w = notify::recommended_watcher(move |res| {
                let _ = notify_tx.blocking_send(res);
            })?;
            let mode = if dir.is_dir() {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            debug!("watching {dir:?}");
            w.watch(dir.as_path(), mode)?;
            Some(w)
        } else {
            None
        };

        std::thread::spawn(move || {
            let _watcher = watcher;
            let mut tails: HashMap<PathBuf, Tail> = HashMap::new();

            if let (Some(position), Some(mut files)) = (start_position, files) {
                files.seek_to(&position.path);

                let sweep_start = Instant::now();
                let mut lines_read = 0usize;

                for path in files {
                    let key = canonical(&path);
                    let mut file = match File::open(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            error!("open {path:?}: {e:?}");
                            continue;
                        }
                    };

                    let mut tail = if key == canonical(&position.path) {
                        if let Err(e) = file.seek(SeekFrom::Start(position.offset as u64)) {
                            error!("seek {path:?}: {e:?}");
                            continue;
                        }
                        Tail {
                            file,
                            accum: Vec::new(),
                            position: Position {
                                path: key.clone(),
                                line_number: position.line_number,
                                offset: position.offset,
                            },
                        }
                    } else {
                        Tail {
                            file,
                            accum: Vec::new(),
                            position: Position {
                                path: key.clone(),
                                line_number: 0,
                                offset: 0,
                            },
                        }
                    };

                    let before = tail.position.line_number;
                    match read_and_emit(&mut tail, &line_tx, &metrics) {
                        Ok(true) => {
                            lines_read += tail.position.line_number - before;
                            tails.insert(key, tail);
                        }
                        Ok(false) => return,
                        Err(e) => eprintln!("read {path:?}: {e:?}"),
                    }
                }

                info!(
                    "initial sweep complete: read {lines_read} lines in {:.2?}",
                    sweep_start.elapsed()
                );
            }

            if !follow {
                return;
            }

            while let Some(res) = notify_rx.blocking_recv() {
                let event = match res {
                    Ok(ev) => ev,
                    Err(e) => {
                        eprintln!("watch error: {e:?}");
                        continue;
                    }
                };
                if !matches!(event.kind, EventKind::Modify(ModifyKind::Data(_))) {
                    continue;
                }

                for path in &event.paths {
                    let key = canonical(path);
                    let tail = match tails.entry(key.clone()) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(slot) => {
                            let file = match File::open(path) {
                                Ok(f) => f,
                                Err(e) => {
                                    eprintln!("open {path:?}: {e:?}");
                                    continue;
                                }
                            };

                            slot.insert(Tail {
                                file,
                                accum: Vec::new(),
                                position: Position {
                                    path: key,
                                    line_number: 0,
                                    offset: 0,
                                },
                            })
                        }
                    };

                    match read_and_emit(tail, &line_tx, &metrics) {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(e) => eprintln!("read {path:?}: {e:?}"),
                    }
                }
            }
        });

        Ok(line_rx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempfile;
    use tokio::sync::mpsc;

    use crate::{Event, Metrics, Position, Tail, read_and_emit};

    #[test]
    fn test_read_and_emit() {
        let (line_tx, mut line_rx) = mpsc::channel::<Event>(1024);

        let mut file = tempfile().unwrap();

        write!(file, "alpha\nbeta\ngamma\n").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut tail = Tail {
            accum: Vec::new(),
            file,
            position: Position {
                path: "test.log".into(),
                line_number: 0,
                offset: 0,
            },
        };

        let completed = read_and_emit(&mut tail, &line_tx, &Metrics::new()).unwrap();

        assert!(completed);

        drop(line_tx);

        let mut lines = Vec::new();
        while let Ok(ev) = line_rx.try_recv() {
            lines.push(ev.line);
        }

        assert_eq!(
            lines,
            vec![b"alpha\n".to_vec(), b"beta\n".to_vec(), b"gamma\n".to_vec()]
        );
        assert_eq!(tail.position.line_number, 3);
        assert_eq!(tail.position.offset, "alpha\nbeta\ngamma\n".len());
    }
}
