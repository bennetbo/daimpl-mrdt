use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mrdt_rs::*;

fn set_merge(iterations: u64) -> HashSet<u64> {
    let mut lca = HashSet::default();

    for i in 0..iterations {
        let mut set1 = lca.clone();
        let mut set2 = lca.clone();

        set1.insert(i);
        set2.insert(i + 1);

        lca = Mergeable::merge(&lca, &set1, &set2);
    }

    lca
}

fn list_merge(iterations: u64) -> Vec<u64> {
    let mut lca = Vec::default();

    for i in 0..iterations {
        let mut list1 = lca.clone();
        let mut list2 = lca.clone();

        list1.push(i);
        list2.push(i + 1);

        lca = Mergeable::merge(&lca, &list1, &list2);
    }

    lca
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut small = c.benchmark_group("merge_small");
    small.bench_function("set_merge 1000", |b| b.iter(|| set_merge(black_box(1000))));
    small.bench_function("list_merge 1000", |b| {
        b.iter(|| list_merge(black_box(1000)))
    });
    small.finish();

    let mut large = c.benchmark_group("merge_large");
    large.bench_function("set_merge 10000", |b| {
        b.iter(|| set_merge(black_box(10000)))
    });
    large.bench_function("list_merge 10000", |b| {
        b.iter(|| list_merge(black_box(10000)))
    });
    large.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
