use mrdt_rs::*;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let hostname = env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let store = Store::setup(hostname, "test").await.unwrap();

    store.dump_table_counts().await?;

    let mut list = Vec::with_capacity(1000);
    for i in 0..1000 {
        list.push(i);
    }

    // TODO: It is unclear how to handle different replicas without a "common" ancestor, we start
    // by manually establishing a base commit
    let main_replica = Id::gen();
    let base_ref = store.insert(&list).await.unwrap();
    let _base_commit = store
        .commit(main_replica, VectorClock::default(), base_ref)
        .await
        .unwrap();
    store.dump_table_counts().await?;

    let mut replica = Replica::clone(main_replica, store).await.unwrap();

    let mut counter = 1000;

    loop {
        for i in 0..10 {
            list.push(counter + i);
        }
        replica.commit_object(&list).await?;
        counter += 10;

        replica.store().dump_table_counts().await?;
    }
}
