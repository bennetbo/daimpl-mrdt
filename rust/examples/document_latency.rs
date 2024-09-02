use mrdt_rs::*;
use musli::{Decode, Encode};
use rand::Rng;
use std::{
    fmt::Display,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tokio::time::interval;

#[derive(Default, Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
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
        let mut document = Self::default();
        document.append_str(value);
        document
    }

    pub fn append(&mut self, value: char) {
        self.contents.push(Character {
            id: Id::gen(),
            value,
        });
    }

    pub fn append_str(&mut self, value: &str) {
        for c in value.chars() {
            self.append(c);
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

impl Mergeable for Document {
    fn merge(lca: &Self, left: &Self, right: &Self) -> Self {
        let contents = Mergeable::merge(&lca.contents, &left.contents, &right.contents);
        Self { contents }
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
    env_logger::init();

    let args = Args::parse();
    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    if args.setup {
        println!("Setting up database...");
        let store = QuarkStore::setup(hostname.clone(), "test").await.unwrap();
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

        let gen_ids = args.gen_ids.unwrap_or(2);
        let ids = (0..gen_ids)
            .map(|_| Id::gen().to_string())
            .collect::<Vec<_>>()
            .join(",");
        for i in 0..gen_ids {
            println!(
                "cargo r --release --example document_latency -- --replicas {ids} --index {i}"
            );
        }
        println!("Or with tracing...");
        for i in 0..gen_ids {
            println!(
                "RUST_LOG=debug cargo r --release --example document_latency -- --replicas {ids} --index {i}"
            );
        }
        return Ok(());
    }

    static STOP: AtomicBool = AtomicBool::new(false);

    let mut replica_ids = args
        .replicas
        .iter()
        .map(|id| Id::try_from(id.clone()).unwrap())
        .collect::<Vec<_>>();

    let replica_id = replica_ids.remove(args.index);

    let hostname = std::env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let store = QuarkStore::setup(hostname.clone(), "test").await.unwrap();

    let mut replica = Replica::clone(replica_id, store).await.unwrap();

    let mut delay = interval(Duration::from_millis(250));

    let mut document_version = replica.latest_commit().version.clone();
    let mut document: Document = replica.latest_object().await.unwrap().unwrap();

    println!("Running cycles...");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Id>(1);

    let replica_ids_copy = replica_ids.clone();
    // We simulate merging with other replicas by requesting a merge with another replica every 500ms
    let merge_thread = tokio::spawn({
        async move {
            // Wait for 500ms before starting to merge
            tokio::time::sleep(Duration::from_millis(1000)).await;

            let mut delay = interval(Duration::from_millis(5000));

            while !STOP.load(Ordering::Relaxed) {
                for replica_id in replica_ids_copy.iter() {
                    let _ = tx.try_send(replica_id.clone());
                    delay.tick().await;
                }
            }
        }
    });

    let mut latencies = Vec::with_capacity(args.ticks);

    for _ in 0..args.ticks {
        delay.tick().await;

        let latency = Instant::now();

        // Generate a random character
        let random_char = rand::thread_rng().gen_range('a'..='z');

        // Insert the character at a random position
        let random_position = rand::thread_rng().gen_range(0..document.len());
        // document.insert(random_position, random_char);
        document.append(random_char);

        let mut merged_document = None;

        while let Ok(replica_id_to_merge_with) = rx.try_recv() {
            println!(
                "Received merge request for replica {}",
                replica_id_to_merge_with
            );
            let (_, document) = replica
                .merge_with::<Document>(replica_id_to_merge_with)
                .await
                .unwrap();
            merged_document = Some(document);
        }

        let mut measure_latency = false;
        if let Some(merged_document) = merged_document {
            println!("Merge occurred in background, merging with local version...");
            //We need to merge again, because there occured a merge while we were inserting a new character
            let lca_version = VectorClock::lca(&replica.latest_version(), &document_version);
            let lca_commit = replica
                .store()
                .resolve_commit_for_version(lca_version)
                .await
                .unwrap();
            let lca_object = replica
                .store()
                .resolve::<Document>(lca_commit.root_ref)
                .await
                .unwrap()
                .unwrap();

            document = Document::merge(&lca_object, &merged_document, &document);
            measure_latency = true;
        }

        let commit = replica.commit_object(&document).await.unwrap();
        document_version = commit.version.clone();

        if measure_latency {
            let elapsed_ms = latency.elapsed().as_millis();
            latencies.push((document.len(), elapsed_ms));
            println!(
                "Inserted '{}' at position {}, {} ms elapsed",
                random_char, random_position, elapsed_ms
            );
        }
    }

    STOP.store(true, Ordering::Relaxed);
    _ = merge_thread.await;

    println!("Waiting for other replicas to complete...");

    tokio::time::sleep(Duration::from_millis(5000)).await;

    println!("Merging with other replicas...");
    for replica_id in replica_ids.iter() {
        replica
            .merge_with::<Document>(replica_id.clone())
            .await
            .unwrap();
    }
    let document: Document = replica.latest_object().await.unwrap().unwrap();

    std::fs::write(
        format!("data/document_latency_{}.txt", replica_id.to_string()),
        document.to_string(),
    )
    .unwrap();

    std::fs::write(
        format!("data/latencies_{}.txt", replica_id.to_string()),
        latencies
            .iter()
            .map(|&(len, ms)| format!("{} {}", len, ms))
            .collect::<Vec<String>>()
            .join("\n"),
    )
    .unwrap();

    Ok(())
}
