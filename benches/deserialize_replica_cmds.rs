use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use types::BlockData;

fn bench_deserialize(c: &mut Criterion) {
    let mut line = std::fs::read("./benches/data/replica_cmds.json").unwrap();
    if line.ends_with(b"\n") {
        line.pop();
    }

    let mut group = c.benchmark_group("replica_cmds_deserialize");
    group.throughput(Throughput::Bytes(line.len() as u64));

    group.bench_function("simd_json", |b| {
        b.iter_batched_ref(
            || line.clone(),
            |buf| {
                let block: BlockData = simd_json::serde::from_slice(black_box(buf)).unwrap();
                black_box(block);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_deserialize);
criterion_main!(benches);
