use fxhash::FxHashMap;
use list::MrdtList;
use mrdt_rs::*;
use musli::{Decode, Encode};
use rand::Rng;
use std::{
    fmt::Display,
    time::{Duration, Instant},
};

const CYCLES: usize = 100;
const REPLICAS: usize = 3;
const CHAR_INSERTION_COUNT_PER_CYCLE: usize = 10;

#[derive(Clone, Decode, Encode, Hash, Default, PartialEq, Eq, Debug)]
struct Document {
    contents: MrdtList<Character>,
}

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Character {
    id: Id,
    value: char,
}

impl Document {
    pub fn from_str(value: &str) -> Self {
        let mut document = Self {
            contents: MrdtList::default(),
        };
        document.append_str(value);
        document
    }

    pub fn append(&mut self, value: char) {
        self.contents.add(Character {
            id: Id::gen(),
            value,
        });
    }

    pub fn append_str(&mut self, value: &str) {
        for c in value.chars() {
            self.append(c);
        }
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

    pub fn insert_str(&mut self, idx: usize, value: &str) {
        for (i, c) in value.chars().enumerate() {
            self.insert(idx + i, c);
        }
    }

    pub fn remove(&mut self, idx: usize) {
        self.contents.remove_at(idx);
    }

    pub fn remove_str(&mut self, idx: usize, len: usize) {
        for _ in 0..len {
            self.remove(idx);
        }
    }

    pub fn len(&self) -> usize {
        self.contents.len()
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

impl Mergeable<Document> for Document {
    fn merge(lca: &Document, left: &Document, right: &Document) -> Document {
        Document {
            contents: MrdtList::merge(&lca.contents, &left.contents, &right.contents),
        }
    }
}

impl Entity for Document {
    fn table_name() -> &'static str {
        "document"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const INITIAL_TEXT: &str = "---";

    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Setting up datastores...");
    let mut stores = Vec::with_capacity(REPLICAS);
    for _ in 0..REPLICAS + 1 {
        let store = setup_store(hostname.clone(), "test").await.unwrap();
        stores.push(Some(store));
    }

    let mut base_store = stores[0].take().unwrap();
    // TODO: It is unclear how to handle different replicas without a "common" ancestor, we start
    // by manually establishing a base commit
    let main_replica = Id::gen();
    let base_set_ref = base_store
        .insert(&Document::from_str(INITIAL_TEXT))
        .await
        .unwrap();
    let _base_commit = base_store
        .commit(main_replica, VectorClock::default(), base_set_ref)
        .await
        .unwrap();

    let mut replicas = Vec::with_capacity(REPLICAS);
    let mut replica_ids = Vec::with_capacity(REPLICAS);
    for i in 0..REPLICAS {
        let store = stores[i + 1].take().unwrap();
        let replica = Replica::clone(Id::gen(), store).await.unwrap();
        replica_ids.push(replica.id());
        replicas.push(replica);
    }

    println!("Running cycles...");
    for cycle in 0..CYCLES {
        let instant = Instant::now();
        for (replica_ix, replica) in replicas.iter_mut().enumerate() {
            let merge_with_replica_ix = if replica_ix == 0 {
                REPLICAS - 1
            } else {
                replica_ix - 1
            };

            replica
                .merge_with::<Document>(replica_ids[merge_with_replica_ix])
                .await
                .unwrap();
            let mut document: Document = replica.latest_object().await.unwrap();

            let mut rng = rand::thread_rng();
            let char = (replica_ix as u8 + b'a') as char;
            for _ in 0..CHAR_INSERTION_COUNT_PER_CYCLE {
                document.insert(rng.gen_range(0..document.len()), char);
            }
            replica.commit_object(&document).await.unwrap();
        }
        let elapsed_ms = instant.elapsed().as_millis();
        println!("cycle {} took {} ms", cycle + 1, elapsed_ms);
    }

    println!("Finalizing cycles...");
    for replica in replicas[0..REPLICAS - 1].iter_mut() {
        replica
            .merge_with::<Document>(replica_ids[REPLICAS - 1])
            .await
            .unwrap();
    }

    let mut document_contents = Vec::with_capacity(REPLICAS);
    for replica in replicas.iter_mut() {
        let document: Document = replica.latest_object().await.unwrap();
        document_contents.push(document.to_string());
    }

    println!("Asserting consistency...");
    for (i, document) in document_contents.iter().enumerate() {
        let ix_to_compare = if i + 1 == REPLICAS { 0 } else { i + 1 };
        assert_eq!(document, &document_contents[ix_to_compare]);
    }

    let replica = replicas.iter_mut().next().unwrap();
    let document: Document = replica.latest_object().await.unwrap();
    let text = document.to_string();

    assert_eq!(
        text.len() - INITIAL_TEXT.len(),
        CYCLES * REPLICAS * CHAR_INSERTION_COUNT_PER_CYCLE
    );

    let character_count = count_chars(&text);
    assert_eq!(character_count.len() - 1, REPLICAS);
    character_count.iter().for_each(|(c, count)| {
        if *c == '-' {
            assert_eq!(*count, INITIAL_TEXT.len());
        } else {
            assert_eq!(*count, CYCLES * CHAR_INSERTION_COUNT_PER_CYCLE);
        }
    });
    println!("Completed!");

    Ok(())
}

fn count_chars(s: &str) -> FxHashMap<char, usize> {
    s.chars().fold(FxHashMap::default(), |mut map, c| {
        *map.entry(c).or_insert(0) += 1;
        map
    })
}
