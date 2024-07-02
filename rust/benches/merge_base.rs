use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rb_tree::RBTree;

fn merge_rbtree(iterations: u64) -> RBTree<u64> {
    let mut lca = RBTree::new();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let s1: RBTree<u64> = left
            .intersection(&right)
            .cloned()
            .collect::<RBTree<_>>()
            .intersection(&lca)
            .cloned()
            .collect();
        // s2 = v1 - v0
        let s2 = left.difference(&lca).cloned().collect();
        // s3 = v2 - v0
        let s3 = right.difference(&lca).cloned().collect();

        // result = (s1 ∪ s2) ∪ s3
        lca = s1
            .union(&s2)
            .cloned()
            .collect::<RBTree<_>>()
            .union(&s3)
            .cloned()
            .collect();
    }

    lca
}

fn merge_rbtree_opt(iterations: u64) -> RBTree<u64> {
    let mut lca = RBTree::new();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values: RBTree<u64> = left
            .intersection(&right)
            .filter(|&item| lca.contains(item))
            .cloned()
            .collect();
        // s2 = v1 - v0
        values.extend(left.difference(&lca));
        values.extend(right.difference(&lca));

        lca = values
    }

    lca
}

fn merge_set_smarter(iterations: u64) -> std::collections::HashSet<u64> {
    let mut lca = std::collections::HashSet::default();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values = lca
            .intersection(&left)
            .filter(|&item| right.contains(item))
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        values.extend(left.difference(&lca));
        values.extend(right.difference(&lca));

        lca = values;
    }

    lca
}

fn merge_set(iterations: u64) -> std::collections::HashSet<u64> {
    let mut lca = std::collections::HashSet::default();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values = std::collections::HashSet::default();
        for value in lca.iter() {
            if left.contains(value) && right.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in left.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in right.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        lca = values;
    }

    lca
}

fn merge_set_im(iterations: u64) -> im::HashSet<u64> {
    let mut lca = im::HashSet::default();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values = im::HashSet::default();
        for value in lca.iter() {
            if left.contains(value) && right.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in left.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in right.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        lca = values;
    }

    lca
}

fn merge_set_fx(iterations: u64) -> fxhash::FxHashSet<u64> {
    let mut lca = fxhash::FxHashSet::default();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values = fxhash::FxHashSet::default();
        for value in lca.iter() {
            if left.contains(value) && right.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in left.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        for value in right.iter() {
            if !lca.contains(value) {
                values.insert(value.clone());
            }
        }
        lca = values;
    }

    lca
}

fn merge_set_fx_faster(iterations: u64) -> fxhash::FxHashSet<u64> {
    let mut lca = fxhash::FxHashSet::default();

    for i in 0..iterations {
        let mut left = lca.clone();
        let mut right = lca.clone();
        left.insert(i);
        right.insert(i + 1);

        let mut values = lca
            .intersection(&left)
            .filter(|&item| right.contains(item))
            .cloned()
            .collect::<fxhash::FxHashSet<_>>();

        values.extend(left.difference(&lca));
        values.extend(right.difference(&lca));
        lca = values;
    }

    lca
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut small = c.benchmark_group("merge_small");
    small.bench_function("merge_rbtree 100", |b| {
        b.iter(|| merge_rbtree(black_box(100)))
    });
    small.bench_function("merge_rbtree_opt 100", |b| {
        b.iter(|| merge_rbtree_opt(black_box(100)))
    });
    small.bench_function("merge_set 100", |b| b.iter(|| merge_set(black_box(100))));
    small.bench_function("merge_set_im 100", |b| {
        b.iter(|| merge_set_im(black_box(100)))
    });
    small.bench_function("merge_set_smarter 100", |b| {
        b.iter(|| merge_set_smarter(black_box(100)))
    });
    small.bench_function("merge_set_fx 100", |b| {
        b.iter(|| merge_set_fx(black_box(100)))
    });
    small.bench_function("merge_set_fx_faster 100", |b| {
        b.iter(|| merge_set_fx_faster(black_box(100)))
    });
    small.finish();

    let mut medium = c.benchmark_group("merge_medium");
    // medium.bench_function("merge_rbtree 1000", |b| {
    //     b.iter(|| merge_rbtree(black_box(1000)))
    // });
    // medium.bench_function("merge_rbtree_opt 1000", |b| {
    //     b.iter(|| merge_rbtree_opt(black_box(1000)))
    // });
    // medium.bench_function("merge_set 1000", |b| b.iter(|| merge_set(black_box(1000))));
    // medium.bench_function("merge_set_im 1000", |b| {
    //     b.iter(|| merge_set_im(black_box(1000)))
    // });
    // medium.bench_function("merge_set_smarter 1000", |b| {
    //     b.iter(|| merge_set_smarter(black_box(1000)))
    // });
    medium.bench_function("merge_set_fx 1000", |b| {
        b.iter(|| merge_set_fx(black_box(1000)))
    });
    medium.bench_function("merge_set_fx_faster 1000", |b| {
        b.iter(|| merge_set_fx_faster(black_box(1000)))
    });
    medium.finish();

    let mut large = c.benchmark_group("merge_large");
    // large.bench_function("merge_rbtree 10000", |b| {
    //     b.iter(|| merge_rbtree(black_box(10000)))
    // });
    // large.bench_function("merge_rbtree_opt 10000", |b| {
    //     b.iter(|| merge_rbtree_opt(black_box(10000)))
    // });
    // large.bench_function("merge_set 10000", |b| {
    //     b.iter(|| merge_set(black_box(10000)))
    // });
    // large.bench_function("merge_set_im 10000", |b| {
    //     b.iter(|| merge_set_im(black_box(10000)))
    // });
    // large.bench_function("merge_set_smarter 10000", |b| {
    //     b.iter(|| merge_set_smarter(black_box(10000)))
    // });
    large.bench_function("merge_set_fx 10000", |b| {
        b.iter(|| merge_set_fx(black_box(10000)))
    });
    large.bench_function("merge_set_fx_faster 10000", |b| {
        b.iter(|| merge_set_fx_faster(black_box(10000)))
    });
    large.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
