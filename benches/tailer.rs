use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tailer::{Position, Tailer, collector::FilesCollector};

fn bench_tailer(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let data = std::fs::read("./benches/data/replica_cmds_50_lines.json").unwrap();
    let file = dir.path().join("replica_cmds_50_lines.json");
    std::fs::write(&file, &data).unwrap();

    let lines = data.iter().filter(|&&b| b == b'\n').count() as u64;

    let mut group = c.benchmark_group("replica_cmds_tailer");
    group.throughput(Throughput::Elements(lines));

    group.bench_function("tail file", |b| {
        b.iter(|| {
            let mut tailer = Tailer::new(dir.path().to_path_buf());
            tailer.with_files(
                FilesCollector::new(dir.path().to_path_buf(), |a, b| a.cmp(b)).unwrap(),
            );
            tailer.with_start_position(Position {
                path: file.clone(),
                line_number: 0,
                offset: 0,
            });

            let mut rx = tailer.run().unwrap();
            while let Some(event) = rx.blocking_recv() {
                black_box(&event);
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_tailer);
criterion_main!(benches);
