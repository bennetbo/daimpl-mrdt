use list::MrdtList;
use mrdt_rs::*;
use musli::{Decode, Encode};
use rand::Rng;
use std::{fmt::Display, time::Duration};
use tokio::time::interval;

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

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The index that points to the replica id to use
    #[clap(long, default_value_t = 0)]
    index: usize,

    /// Number of ticks (insertions)
    #[clap(long, default_value_t = 240)]
    ticks: usize,

    /// List of replica IDs
    #[clap(long, value_delimiter = ',')]
    replicas: Vec<String>,

    /// Run setup process
    #[clap(long)]
    setup: bool,

    /// Generate random ids
    #[clap(long)]
    gen_ids: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    if let Some(gen_ids) = args.gen_ids {
        for _ in 0..gen_ids {
            println!("{}", Id::gen().to_string());
        }
        return Ok(());
    }

    if args.setup {
        println!("Setting up database...");
        let mut store = setup_store(hostname.clone(), "test").await.unwrap();
        println!("Connected to database!");
        let main_replica = Id::gen();
        let document = Document::from_str(include_str!("../data/text.txt"));
        println!("Created document");
        let base_set_ref = store.insert(&document).await.unwrap();
        let _base_commit = store
            .commit(main_replica, VectorClock::default(), base_set_ref)
            .await
            .unwrap();
        println!("Setup complete!");
        return Ok(());
    }

    let mut replica_ids = args
        .replicas
        .iter()
        .map(|id| Id::try_from(id.clone()).unwrap())
        .collect::<Vec<_>>();

    let replica_id = replica_ids.remove(args.index);

    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let store = setup_store(hostname.clone(), "test").await.unwrap();

    let mut replica = Replica::clone(replica_id, store).await.unwrap();

    let mut interval = interval(Duration::from_millis(250));

    let mut document: Document = replica.latest_object().await.unwrap();

    println!("Running cycles...");

    for _ in 0..args.ticks {
        interval.tick().await;

        // Generate a random character
        let random_char = rand::thread_rng().gen_range('a'..='z');

        // Insert the character at a random position
        let random_position = rand::thread_rng().gen_range(0..document.len());
        document.insert(random_position, random_char);

        replica.commit_object(&document).await?;

        println!("Inserted '{}' at position {}", random_char, random_position);
    }

    std::fs::write("data/document.txt", document.to_string()).unwrap();

    Ok(())
}
