use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mrdt_rs::*;

fn set_insert(iterations: u64) -> HashSet<u64> {
    let mut set = HashSet::default();
    for i in 0..iterations {
        set.insert(i);
    }
    set
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("set_insert 1000", |b| {
        b.iter(|| set_insert(black_box(1000)))
    });
    c.bench_function("set_insert 10000", |b| {
        b.iter(|| set_insert(black_box(10000)))
    });
    c.bench_function("set_insert 100000", |b| {
        b.iter(|| set_insert(black_box(100000)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
