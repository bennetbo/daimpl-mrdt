use mrdt_rs::*;
use musli::{Decode, Encode};
use std::fmt::Display;

const CYCLES: usize = 25;
const CHAR_INSERTION_COUNT_PER_CYCLE: usize = 10;

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
    pub fn from_str(value: &str) -> Self {
        let mut document = Self {
            contents: Vec::default(),
        };
        for c in value.chars() {
            document.append(c);
        }
        document
    }

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
    fn merge(lca: &Document, left: &Document, right: &Document) -> Document {
        Document {
            contents: Mergeable::merge(&lca.contents, &left.contents, &right.contents),
        }
    }
}

impl Serialize for Document {
    async fn serialize(&self, cx: SerializeCx<'_>) -> Result<Vec<Ref>> {
        self.contents.serialize(cx).await
    }
}

impl Deserialize for Document {
    async fn deserialize(root: Ref, cx: DeserializeCx<'_>) -> Result<Self> {
        let contents = Vec::deserialize(root, cx).await?;
        Ok(Self { contents })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    async fn setup_replica() -> Result<Replica> {
        let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let store = QuarkStore::setup(hostname.clone(), "test").await.unwrap();

        let main_replica = Id::gen();
        let base_set_ref = store.insert(&Document::from_str(".")).await?;
        store
            .commit(main_replica, VectorClock::default(), base_set_ref)
            .await?;

        let replica = Replica::clone(main_replica, store).await?;
        Ok(replica)
    }

    async fn run_cycles(replica: &mut Replica, insert_at_start: bool) -> Result<Vec<(usize, u64)>> {
        println!(
            "{}",
            if insert_at_start {
                "Inserting at start..."
            } else {
                "Appending at end..."
            }
        );
        let mut reference_counts = Vec::with_capacity(CYCLES);
        let mut document = replica.latest_object::<Document>().await?.unwrap();
        for _ in 0..CYCLES {
            for _ in 0..CHAR_INSERTION_COUNT_PER_CYCLE {
                if insert_at_start {
                    document.insert(0, 'a');
                } else {
                    document.append('a');
                }
            }
            replica.commit_object(&document).await?;
            let table_counts = replica.store().table_counts().await?;
            println!(
                "commits: {}, objects: {}, refs: {}, replicas: {}",
                table_counts.commits,
                table_counts.objects,
                table_counts.refs,
                table_counts.replicas
            );
            reference_counts.push((document.len(), table_counts.refs));
        }
        dbg!(replica
            .latest_object::<Document>()
            .await?
            .unwrap()
            .to_string());
        Ok(reference_counts)
    }

    let mut replica = setup_replica().await.unwrap();
    let ref_counts_when_appending = run_cycles(&mut replica, false).await.unwrap();

    replica.store().reset_db().await.unwrap();

    let mut replica = setup_replica().await.unwrap();
    let ref_counts_when_prepending = run_cycles(&mut replica, true).await.unwrap();

    std::fs::write(
        "data/append_document_ref_counts.txt",
        ref_counts_when_appending
            .iter()
            .map(|&(len, ref_count)| format!("{} {}", len, ref_count))
            .collect::<Vec<String>>()
            .join("\n"),
    )
    .unwrap();
    std::fs::write(
        "data/prepend_document_ref_counts.txt",
        ref_counts_when_prepending
            .iter()
            .map(|&(len, ref_count)| format!("{} {}", len, ref_count))
            .collect::<Vec<String>>()
            .join("\n"),
    )
    .unwrap();

    Ok(())
}
