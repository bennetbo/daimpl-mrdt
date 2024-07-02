use criterion::{black_box, criterion_group, criterion_main, Criterion};
use list::MrdtList;
use mrdt_rs::*;

fn set_merge(iterations: u64) -> MrdtSet<u64> {
    let mut lca = MrdtSet::default();

    for i in 0..iterations {
        let mut set1 = lca.clone();
        let mut set2 = lca.clone();

        set1.insert(i);
        set2.insert(i + 1);

        lca = MrdtSet::merge(&lca, &set1, &set2);
    }

    lca
}

fn list_merge(iterations: u64) -> MrdtList<u64> {
    let mut lca = MrdtList::default();

    for i in 0..iterations {
        let mut list1 = lca.clone();
        let mut list2 = lca.clone();

        list1.add(i);
        list2.add(i + 1);

        lca = MrdtList::merge(&lca, &list1, &list2);
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
