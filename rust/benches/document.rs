use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mrdt_rs::*;
use musli::{Decode, Encode};
use rand::Rng;
use std::fmt::Display;

#[derive(Clone, Decode, Encode, Hash, Default, PartialEq, Eq, Debug)]
struct Document {
    contents: Vec<Character>,
}

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Character {
    id: Id,
    value: char,
}

impl Document {
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn append(&mut self, value: char) {
        self.contents.push(Character {
            id: Id::gen(),
            value,
        });
    }

    pub fn insert(&mut self, idx: usize, value: char) {
        self.contents.insert(
            idx,
            Character {
                id: Id::gen(),
                value,
            },
        )
    }
}

impl Display for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.contents.iter().map(|c| c.value).collect::<String>()
        )
    }
}

impl Mergeable for Document {
    fn merge(lca: &Self, left: &Self, right: &Self) -> Self {
        let contents = Mergeable::merge(&lca.contents, &left.contents, &right.contents);
        Document { contents }
    }
}

fn document_merge(lca: &Document, left: &Document, right: &Document) -> Document {
    Document::merge(lca, left, right)
}

fn document_with_random_chars(document_length: usize) -> Document {
    let mut document = Document::default();
    for _ in 0..document_length {
        document.append(rand::random::<char>());
    }
    document
}

fn document_insert_random_chars(document: &mut Document, insertion_count: usize) {
    for _ in 0..insertion_count {
        document.insert(
            rand::thread_rng().gen_range(0..document.len()),
            rand::random::<char>(),
        );
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("document_merge");

    let sizes: &[(usize, usize)] = &[(100, 10), (1000, 100), (10000, 1000), (100000, 10000)];

    for (len, insertion_count) in sizes {
        let lca = document_with_random_chars(*len);
        let mut left = lca.clone();
        let mut right = lca.clone();
        document_insert_random_chars(&mut left, *insertion_count);
        document_insert_random_chars(&mut right, *insertion_count);

        group.bench_function(
            format!(
                "document_merge length: {} insertions: {}",
                len, insertion_count
            ),
            |b| b.iter(|| document_merge(black_box(&lca), black_box(&left), black_box(&right))),
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
