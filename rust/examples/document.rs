use list::MrdtList;
use mrdt_rs::*;
use musli::{Decode, Encode};
use std::fmt::Display;

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
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
    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let mut store1 = setup_store(hostname.clone(), "test").await.unwrap();
    let store2 = setup_store(hostname.clone(), "test").await.unwrap();
    // let store3 = setup_store(hostname, "test").await.unwrap();

    // TODO: It is unclear how to handle different replicas without a "common" ancestor, we start
    // by manually establishing a base commit
    let main_replica = Id::gen();
    let base_set_ref = store1
        .insert(&Document::from_str("Hello World!\n"))
        .await
        .unwrap();
    let _base_commit = store1
        .commit(main_replica, VectorClock::default(), base_set_ref)
        .await
        .unwrap();

    let mut replica1 = Replica::clone(Id::gen(), store1).await.unwrap();
    let mut replica2 = Replica::clone(Id::gen(), store2).await.unwrap();
    // let mut replica3 = Replica::clone(Id::gen(), store3).await.unwrap();

    // for i in 0..1 {
    //     replica1
    //         .merge_with::<Document>(replica3.id())
    //         .await
    //         .unwrap();
    //     let mut document1: Document = replica1.latest_object().await.unwrap();
    //     document1.append_str(&format!("Replica 1: {i}\n"));
    //     replica1.commit_object(&document1).await.unwrap();

    //     dbg!(document1.to_string());

    //     let mut document2: Document = replica2.latest_object().await.unwrap();
    //     document2.append_str(&format!("Replica 2: {i}\n"));
    //     replica2.commit_object(&document2).await.unwrap();
    //     replica2
    //         .merge_with::<Document>(replica1.id())
    //         .await
    //         .unwrap();

    //     dbg!(document2.to_string());
    //     let mut document2: Document = replica2.latest_object().await.unwrap();
    //     dbg!(document2.to_string());

    //     let mut document3: Document = replica3.latest_object().await.unwrap();
    //     document3.append_str(&format!("Replica 3: {i}\n"));
    //     replica3.commit_object(&document3).await.unwrap();
    //     replica3
    //         .merge_with::<Document>(replica1.id())
    //         .await
    //         .unwrap();

    //     dbg!(document3.to_string());
    // }

    let mut document1: Document = replica1.latest_object().await.unwrap();
    let mut document2: Document = replica2.latest_object().await.unwrap();

    // let document3: Document = replica3.latest_object().await.unwrap();

    dbg!(document1.to_string());
    dbg!(document2.to_string());

    document1.append_str("Replica 1\n");
    document2.append_str("Replica 2\n");

    dbg!(document1.to_string());
    dbg!(document2.to_string());

    dbg!(replica1.commit_object(&document1).await.unwrap());
    dbg!(replica1.commit_object(&document2).await.unwrap());

    let document1: Document = replica1.latest_object().await.unwrap();
    let document2: Document = replica2.latest_object().await.unwrap();

    dbg!(document1.to_string());
    dbg!(document2.to_string());

    replica2
        .merge_with::<Document>(replica1.id())
        .await
        .unwrap();

    let document2: Document = replica2.latest_object().await.unwrap();
    dbg!(document2.to_string());

    Ok(())
}
