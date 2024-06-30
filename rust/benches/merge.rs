use criterion::{black_box, criterion_group, criterion_main, Criterion};
use list::MrdtList;
use mrdt_rs::*;

fn set_merge(iterations: u64) -> MrdtSet<u64> {
    let mut base_set = MrdtSet::default();

    for i in 0..iterations {
        let set1 = base_set.insert(i);
        let set2 = base_set.insert(i + 1);

        base_set = MrdtSet::merge(&base_set, &set1, &set2);
    }

    base_set
}

fn list_merge(iterations: u64) -> MrdtList<u64> {
    let mut base_list = MrdtList::default();

    for i in 0..iterations {
        let list1 = base_list.add(i);
        let list2 = base_list.add(i + 1);

        base_list = MrdtList::merge(&base_list, &list1, &list2);
    }

    base_list
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
