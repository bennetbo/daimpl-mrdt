use musli::{storage::Encoding, Options};
pub use object_store::*;
pub use ref_store::*;

pub mod object_store;
pub mod ref_store;

pub use super::*;

use scylla::{query::Query, serialize::row::SerializeRow, QueryResult, Session, SessionBuilder};

const OPTIONS: Options = musli::options::new().build();

const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

pub async fn setup_store(
    hostname: impl Into<String>,
    keyspace: impl Into<String>,
) -> Result<Store> {
    let hostname = hostname.into();
    let keyspace = keyspace.into();

    let session: Session = SessionBuilder::new().known_node(hostname).build().await?;
    session.query(format!("CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"), ()).await?;

    let mut this = Store::new(keyspace, session);
    object_store::create_tables(&mut this).await?;
    ref_store::create_tables(&mut this).await?;
    Ok(this)
}

pub struct Store {
    keyspace: String,
    session: Session,
}

impl Store {
    pub fn new(keyspace: String, session: Session) -> Self {
        Self { keyspace, session }
    }

    pub async fn query(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<QueryResult> {
        self.session
            .query(query, values)
            .await
            .with_context(|| "Failed to execute query")
    }

    pub fn table_name(&self, name: &str) -> String {
        format!("{}.{}", self.keyspace, name)
    }
}
