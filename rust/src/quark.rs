use anyhow::{Context, Result};
use musli::{
    de::DecodeOwned,
    mode::Binary,
    storage::{Encoding, OPTIONS},
    Encode,
};
use scylla::{query::Query, serialize::row::SerializeRow, QueryResult, Session, SessionBuilder};
use std::hash::{Hash, Hasher};

const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

#[derive(Clone, Debug)]
pub struct Ref {
    pub id: u64,
    pub left: Option<u64>,
    pub right: Option<u64>,
    pub object_ref: u64,
}

pub struct ScyllaSession {
    session: Session,
    keyspace: String,
}

impl ScyllaSession {
    pub async fn new(hostname: impl Into<String>, keyspace: impl Into<String>) -> Result<Self> {
        let hostname = hostname.into();
        let keyspace = keyspace.into();

        let session: Session = SessionBuilder::new().known_node(hostname).build().await?;
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"), ()).await?;

        Ok(Self { session, keyspace })
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

pub enum Store {
    Scylla(ScyllaSession),
    #[cfg(test)]
    Fake {
        objects: std::cell::RefCell<std::collections::HashMap<u64, Vec<u8>>>,
        refs: std::cell::RefCell<std::collections::HashMap<u64, Ref>>,
    },
}

const REF_TABLE_NAME: &str = "ref";
const OBJECT_TABLE_NAME: &str = "object";

impl Store {
    pub async fn setup(hostname: impl Into<String>, keyspace: impl Into<String>) -> Result<Store> {
        let scylla_session = ScyllaSession::new(hostname, keyspace).await?;

        let ref_table_name = scylla_session.table_name(REF_TABLE_NAME);
        scylla_session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {ref_table_name}
                        (id BIGINT NOT NULL, left BIGINT, right BIGINT, object_ref BIGINT NOT NULL,
                        PRIMARY KEY (id))"
                ),
                &[],
            )
            .await?;

        let object_table_name = scylla_session.table_name(OBJECT_TABLE_NAME);
        scylla_session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {object_table_name}
                        (id BIGINT NOT NULL, object BLOB,
                        PRIMARY KEY (id))"
                ),
                &[],
            )
            .await?;

        Ok(Self::Scylla(scylla_session))
    }

    #[cfg(test)]
    pub fn test() -> Self {
        Self::Fake {
            objects: std::cell::RefCell::new(std::collections::HashMap::new()),
            refs: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    pub async fn insert_t<T: Serialize>(&self, object: &T) -> Result<()> {
        let cx = SerializeCx { store: self };
        let references = object.serialize(cx).await?;

        match self {
            Store::Scylla(session) => {
                session
                    .query(
                        "INSERT INTO object (id, left, right, object_ref) VALUES (?, ?, ?, ?)",
                        references
                            .iter()
                            .map(|reference| {
                                (
                                    reference.id as i64,
                                    reference.left.map(|id| id as i64),
                                    reference.right.map(|id| id as i64),
                                    reference.object_ref as i64,
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                Ok(())
            }
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let mut refs = refs.borrow_mut();
                for reference in references {
                    refs.insert(reference.id, reference);
                }
                Ok(())
            }
        }
    }

    pub async fn resolve_t<T: Deserialize>(&self, root: u64) -> Result<Option<T>> {
        let Some(root) = self.resolve_ref(Some(root)).await? else {
            return Ok(None);
        };
        let cx = DeserializeCx { store: self };
        let result = T::deserialize(root, cx).await?;
        Ok(Some(result))
    }

    pub async fn resolve<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
        match self {
            Store::Scylla(session) => {
                let object_table = session.table_name(OBJECT_TABLE_NAME);
                let object_blob = session
                    .query(
                        format!("SELECT object FROM {object_table} WHERE id = ?"),
                        (id as i64,),
                    )
                    .await?
                    .single_row()
                    .with_context(|| "Failed to select single row")?
                    .columns[0]
                    .as_ref()
                    .and_then(|value| value.clone().into_blob())
                    .with_context(|| "Failed to deserialize value")?;

                ENCODING
                    .decode(object_blob.as_slice())
                    .with_context(|| "Failed to deserialize object")
            }
            #[cfg(test)]
            Store::Fake { objects, .. } => {
                let objects = objects.borrow();
                let Some(bytes) = objects.get(&id) else {
                    return Ok(None);
                };
                let decoded = ENCODING
                    .decode(bytes.as_slice())
                    .with_context(|| "Failed to deserialize object")?;

                Ok(Some(decoded))
            }
        }
    }

    pub async fn insert<T: Hash + Encode<Binary>>(&self, object: &T) -> Result<u64> {
        match self {
            Store::Scylla(session) => {
                let object_table = session.table_name(OBJECT_TABLE_NAME);

                let mut data = Vec::new();
                ENCODING
                    .encode(&mut data, object)
                    .with_context(|| "Failed to serialize object")?;

                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                let hash = hasher.finish();

                session
                    .query(
                        format!("INSERT INTO {object_table} (id, object) VALUES (?, ?)"),
                        (hash as i64, data),
                    )
                    .await?;

                Ok(hash)
            }
            #[cfg(test)]
            Store::Fake { objects, .. } => {
                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                let hash = hasher.finish();

                let mut bytes = Vec::new();
                ENCODING
                    .encode(&mut bytes, object)
                    .with_context(|| "Failed to serialize object")?;

                let mut objects = objects.borrow_mut();
                objects.insert(hash, bytes);
                Ok(hash)
            }
        }
    }

    #[cfg(test)]
    pub fn insert_ref(&self, reference: Ref) {
        match self {
            Store::Scylla(_) => todo!(),
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let mut refs = refs.borrow_mut();
                refs.insert(reference.id, reference);
            }
        }
    }

    pub async fn resolve_ref(&self, id: Option<u64>) -> Result<Option<Ref>> {
        let Some(id) = id else {
            return Ok(None);
        };

        match self {
            Store::Scylla(session) => {
                let ref_table = session.table_name(REF_TABLE_NAME);
                let result = session
                    .query(
                        format!("SELECT left, right, object_ref FROM {ref_table} WHERE id = ?"),
                        (id.to_string(),),
                    )
                    .await?
                    .single_row()
                    .with_context(|| "Failed to select single row")?;

                let left = result.columns[0]
                    .as_ref()
                    .and_then(|value| value.clone().as_bigint())
                    .map(|id| id as u64);

                let right = result.columns[1]
                    .as_ref()
                    .and_then(|value| value.clone().as_bigint())
                    .map(|id| id as u64);

                let object_ref = result.columns[2]
                    .as_ref()
                    .and_then(|value| value.clone().as_bigint())
                    .with_context(|| "Failed to deserialize object_ref")?
                    as u64;

                Ok(Some(Ref {
                    id,
                    left,
                    right,
                    object_ref,
                }))
            }
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let refs = refs.borrow();
                Ok(refs.get(&id).cloned())
            }
        }
    }
}

pub struct SerializeCx<'a> {
    store: &'a Store,
}

impl SerializeCx<'_> {
    pub async fn insert<T: Hash + Encode<Binary>>(&self, object: &T) -> Result<u64> {
        self.store.insert(object).await
    }
}

pub struct DeserializeCx<'a> {
    store: &'a Store,
}

impl DeserializeCx<'_> {
    pub async fn resolve<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
        self.store.resolve(id).await
    }

    pub async fn resolve_ref(&self, id: Option<u64>) -> Result<Option<Ref>> {
        self.store.resolve_ref(id).await
    }
}

#[allow(async_fn_in_trait)]
pub trait Serialize {
    async fn serialize(&self, cx: SerializeCx) -> Result<Vec<Ref>>;
}

#[allow(async_fn_in_trait)]
pub trait Deserialize: Sized {
    async fn deserialize(root: Ref, cx: DeserializeCx) -> Result<Self>;
}

#[cfg(test)]
mod tests {
    use std::hash::{Hash, Hasher};

    use super::*;

    #[derive(Debug, PartialEq)]
    struct List {
        items: Vec<String>,
    }

    impl Serialize for List {
        async fn serialize(&self, cx: SerializeCx<'_>) -> Result<Vec<Ref>> {
            let mut refs = Vec::with_capacity(self.items.len());
            let mut prev_hash: Option<u64> = None;
            for object in self.items.iter() {
                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                if let Some(last_hash) = prev_hash {
                    last_hash.hash(&mut hasher);
                }
                let hash = hasher.finish();

                let object_ref = cx.insert(object).await?;
                refs.push(Ref {
                    id: hash,
                    left: prev_hash,
                    right: None,
                    object_ref,
                });
                prev_hash = Some(hash);
            }
            Ok(refs)
        }
    }

    impl Deserialize for List {
        async fn deserialize(root: Ref, cx: DeserializeCx<'_>) -> Result<Self> {
            let mut items = Vec::new();
            items.push(
                cx.resolve(root.object_ref)
                    .await?
                    .with_context(|| "Missing object")?,
            );

            let mut node = root;
            while let Some(new_node) = cx.resolve_ref(node.left).await? {
                items.push(
                    cx.resolve(new_node.object_ref)
                        .await?
                        .with_context(|| "Missing object")?,
                );
                node = new_node
            }
            items.reverse();
            Ok(Self { items })
        }
    }

    #[tokio::test]
    async fn test_serialize_deserialize() {
        let list = List {
            items: vec!["1".to_string(), "2".to_string(), "3".to_string()],
        };

        let store = Store::test();

        let refs = list.serialize(SerializeCx { store: &store }).await.unwrap();

        for reference in refs.iter() {
            store.insert_ref(reference.clone());
        }

        let root = refs.last().cloned().unwrap();
        let deserialized = List::deserialize(root, DeserializeCx { store: &store })
            .await
            .unwrap();

        assert_eq!(deserialized, list);
    }
}
