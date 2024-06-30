use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mrdt_rs::*;

fn set_insert(iterations: u64) -> MrdtSet<u64> {
    let mut set = MrdtSet::default();
    for i in 0..iterations {
        set = set.insert(i);
    }
    set
}

fn set_insert_in_place(iterations: u64) -> MrdtSet<u64> {
    let mut set = MrdtSet::default();
    for i in 0..iterations {
        set.insert_in_place(i);
    }
    set
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("set_insert 1000", |b| {
        b.iter(|| set_insert(black_box(1000)))
    });
    c.bench_function("set_insert 100000", |b| {
        b.iter(|| set_insert(black_box(100000)))
    });

    c.bench_function("set_insert_in_place 1000", |b| {
        b.iter(|| set_insert_in_place(black_box(1000)))
    });
    c.bench_function("set_insert_in_place 100000", |b| {
        b.iter(|| set_insert_in_place(black_box(100000)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
