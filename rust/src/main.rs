use mrdt_rs::*;
use std::env;

#[derive(Default, Debug, Hash, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Person {
    pub first_name: String,
    pub last_name: String,
    pub age: i32,
}

impl Person {
    pub fn new(first_name: impl Into<String>, last_name: impl Into<String>, age: i32) -> Self {
        Self {
            first_name: first_name.into(),
            last_name: last_name.into(),
            age,
        }
    }
}

impl Entity for Person {
    fn table_name() -> &'static str {
        "person"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let hostname = env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let mut store1 = setup_store(hostname.clone(), "test").await.unwrap();
    let store2 = setup_store(hostname, "test").await.unwrap();

    // TODO: It is unclear how to handle different replicas without a "common" ancestor, we start
    // by manually establishing a base commit
    let main_replica = Id::gen();
    let base_set = MrdtSet::default().insert(Person::new("Alice", "Johnson", 28));
    let base_set_ref = store1.insert(&base_set).await.unwrap();
    let _base_commit = store1
        .commit(main_replica, VectorClock::default(), base_set_ref)
        .await
        .unwrap();

    let mut replica1 = Replica::clone(Id::gen(), store1).await.unwrap();
    let mut replica2 = Replica::clone(Id::gen(), store2).await.unwrap();

    let set1: MrdtSet<Person> = replica1.latest_object().await.unwrap();
    let set2: MrdtSet<Person> = replica2.latest_object().await.unwrap();

    let set1 = set1.insert(Person::new("Michael", "Smith", 34));
    let set2 = set2.insert(Person::new("Emma", "Davis", 25));

    replica1.commit_object(&set1).await?;
    replica2.commit_object(&set2).await?;

    replica1
        .merge_with::<MrdtSet<Person>>(replica2.id())
        .await?;

    let set1: MrdtSet<Person> = replica1.latest_object().await.unwrap();
    let set2: MrdtSet<Person> = replica2.latest_object().await.unwrap();

    dbg!(set1);
    dbg!(set2);

    Ok(())
}
