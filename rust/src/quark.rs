use anyhow::{anyhow, Context, Result};
use log::log_enabled;
use musli::{
    de::DecodeOwned,
    mode::Binary,
    storage::{Encoding, OPTIONS},
    Encode,
};
use scylla::{
    batch::Batch, prepared_statement::PreparedStatement, query::Query,
    serialize::row::SerializeRow, QueryResult, Session, SessionBuilder,
};
use std::hash::{Hash, Hasher};
use std::{fmt, time::Instant};

#[cfg(test)]
use std::{cell::RefCell, collections::HashMap};

use crate::{Id, ReplicaId, VectorClock};

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

    pub fn raw(&self) -> &Session {
        &self.session
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
        commits: RefCell<HashMap<Id, Commit>>,
        objects: RefCell<HashMap<u64, Vec<u8>>>,
        refs: RefCell<HashMap<u64, Ref>>,
        replicas: RefCell<HashMap<ReplicaId, CommitId>>,
    },
}

pub type CommitId = Id;

#[derive(Debug, Clone)]
pub struct Commit {
    pub id: CommitId,
    pub version: VectorClock,
    pub root_ref: u64,
    pub parent_commit_id: Option<Id>,
}

pub type ObjectRef = u64;

#[allow(async_fn_in_trait)]
pub trait GitLikeStore {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit>;
    async fn commit(
        &self,
        replica_id: ReplicaId,
        version: VectorClock,
        root_ref: u64,
    ) -> Result<Commit>;

    async fn latest_commit_for_replica(&self, replica_id: ReplicaId) -> Result<Option<Commit>>;
    async fn resolve_commit(&self, commit_id: CommitId) -> Result<Commit>;
    async fn resolve_commit_by_version(&self, version: VectorClock) -> Result<Commit>;
}

#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn resolve_object<T: Hash + DecodeOwned<Binary>>(
        &self,
        id: ObjectRef,
    ) -> Result<Option<T>>;
    async fn resolve_objects<T: Hash + DecodeOwned<Binary>>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<T>>>;
    async fn insert_object<T: Hash + Encode<Binary>>(&self, object: &T) -> Result<ObjectRef>;
    async fn insert_objects<'a, T: 'a + Hash + Encode<Binary>>(
        &self,
        objects: &[&'a T],
    ) -> Result<Vec<ObjectRef>>;
}

#[allow(async_fn_in_trait)]
pub trait VersionedStore {
    async fn resolve_versioned<T: Deserialize + fmt::Debug>(&self, root: u64) -> Result<Option<T>>;
    async fn insert_versioned<T: Serialize + fmt::Debug>(&self, object: &T) -> Result<u64>;
}

impl GitLikeStore for Store {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit> {
        log::debug!("Cloning replica {replica_id}");
        match self {
            Store::Scylla(session) => {
                let commit_table = session.table_name(COMMIT_TABLE_NAME);

                let commit_id = session
                    .query(format!("SELECT id FROM {commit_table}"), &[])
                    .await?
                    .rows_or_empty()
                    .first()
                    .and_then(|row| {
                        row.columns[0]
                            .as_ref()
                            .and_then(|value| value.clone().into_string())
                    })
                    .and_then(|raw_id| Id::try_from(raw_id).ok())
                    .with_context(|| "No commits available")?;

                let commit = self.resolve_commit(commit_id).await?;
                update_current_commit_id_for_replica(session, replica_id, commit_id).await?;
                Ok(commit)
            }
            #[cfg(test)]
            Store::Fake {
                replicas, commits, ..
            } => {
                let commits = commits.borrow();
                let commit = commits
                    .values()
                    .next()
                    .with_context(|| "No commits available")?;

                let mut replicas = replicas.borrow_mut();
                replicas.insert(replica_id, commit.id);

                Ok(commit.clone())
            }
        }
    }

    async fn commit(
        &self,
        replica_id: ReplicaId,
        version: VectorClock,
        root_ref: u64,
    ) -> Result<Commit> {
        log::debug!(
            "Replica {replica_id} adding new commit. Ref: {root_ref}, Version: {version:?}"
        );
        match self {
            Store::Scylla(session) => {
                let commit_table = session.table_name(COMMIT_TABLE_NAME);
                let commit_id = Id::gen();
                let prev_commit_id = current_commit_id_for_replica(session, replica_id).await?;
                update_current_commit_id_for_replica(session, replica_id, commit_id).await?;

                let mut version_bytes = Vec::new();
                ENCODING
                    .encode(&mut version_bytes, &version)
                    .with_context(|| "Failed to serialize version")?;

                session.query(
                    format!("INSERT INTO {commit_table} (id, version, root_ref, prev_commit_id) VALUES (?, ?, ?, ?)"),
                    (
                        commit_id.as_str(),
                        &version_bytes,
                        root_ref as i64,
                        prev_commit_id.map(|id| id.to_string()),
                    ),
                )
                .await?;

                Ok(Commit {
                    id: commit_id,
                    version,
                    root_ref,
                    parent_commit_id: prev_commit_id,
                })
            }
            #[cfg(test)]
            Store::Fake {
                commits, replicas, ..
            } => {
                let commit_id = Id::gen();
                let mut replicas = replicas.borrow_mut();
                let prev_commit_id = replicas
                    .get(&replica_id)
                    .with_context(|| "No previous commit available")?
                    .clone();
                replicas.insert(replica_id, commit_id.clone());

                let mut commits = commits.borrow_mut();
                let commit = Commit {
                    id: commit_id,
                    version,
                    root_ref,
                    parent_commit_id: Some(prev_commit_id),
                };
                commits.insert(commit_id, commit.clone());
                Ok(commit)
            }
        }
    }

    async fn latest_commit_for_replica(&self, replica_id: ReplicaId) -> Result<Option<Commit>> {
        match self {
            Store::Scylla(session) => {
                match current_commit_id_for_replica(session, replica_id).await? {
                    Some(commit_id) => Ok(Some(self.resolve_commit(commit_id).await?)),
                    None => Ok(None),
                }
            }
            #[cfg(test)]
            Store::Fake {
                replicas, commits, ..
            } => {
                let replicas = replicas.borrow();
                let Some(commit_id) = replicas.get(&replica_id) else {
                    return Ok(None);
                };
                Ok(commits.borrow().get(commit_id).cloned())
            }
        }
    }

    async fn resolve_commit(&self, commit_id: CommitId) -> Result<Commit> {
        match self {
            Store::Scylla(session) => {
                let commit_table = session.table_name(COMMIT_TABLE_NAME);
                let result = session
                    .query(
                        format!(
                            "SELECT version, root_ref, prev_commit_id FROM {commit_table} WHERE id = ?"
                        ),
                        (commit_id.as_str(),),
                    )
                    .await?
                    .single_row()
                    .with_context(|| "Failed to select single row")?;

                let version_blob = result.columns[0]
                    .as_ref()
                    .and_then(|value| value.clone().into_blob())
                    .with_context(|| "Failed to deserialize value")?;

                let root_ref = result.columns[1]
                    .as_ref()
                    .and_then(|value| value.clone().as_bigint())
                    .with_context(|| "Failed to deserialize root ref")?
                    as u64;

                let prev_commit_id = result.columns[2]
                    .as_ref()
                    .and_then(|value| value.clone().into_string());

                Ok(Commit {
                    id: commit_id,
                    version: ENCODING.decode(version_blob.as_slice())?,
                    root_ref,
                    parent_commit_id: prev_commit_id.and_then(|id| Id::try_from(id).ok()),
                })
            }
            #[cfg(test)]
            Store::Fake { commits, .. } => {
                let commits = commits.borrow();
                commits
                    .get(&commit_id)
                    .with_context(|| "Commit not found")
                    .cloned()
            }
        }
    }

    async fn resolve_commit_by_version(&self, version: VectorClock) -> Result<Commit> {
        match self {
            Store::Scylla(session) => {
                let commit_table = session.table_name(COMMIT_TABLE_NAME);

                let mut version_bytes = Vec::new();
                ENCODING.encode(&mut version_bytes, &version)?;
                let result = session
                    .query(
                        format!(
                            "SELECT id, root_ref, prev_commit_id FROM {commit_table} WHERE version = ? ALLOW FILTERING"
                        ),
                        (&version_bytes,),
                    )
                    .await?
                    .single_row()
                    .with_context(|| "Failed to select single row")?;

                let commit_id = result.columns[0]
                    .as_ref()
                    .and_then(|value| value.clone().into_string())
                    .with_context(|| "Failed to deserialize value")?;

                let root_ref = result.columns[1]
                    .as_ref()
                    .and_then(|value| value.as_bigint())
                    .with_context(|| "Failed to deserialize root ref")?
                    as u64;

                let prev_commit_id = result.columns[2]
                    .as_ref()
                    .and_then(|value| value.clone().into_string());

                Ok(Commit {
                    id: Id::try_from(commit_id)?,
                    version,
                    root_ref,
                    parent_commit_id: prev_commit_id.and_then(|id| Id::try_from(id).ok()),
                })
            }
            #[cfg(test)]
            Store::Fake { commits, .. } => {
                let commits = commits.borrow();
                commits
                    .values()
                    .find(|c| c.version == version)
                    .with_context(|| "Failed to resolve commit for version")
                    .cloned()
            }
        }
    }
}

async fn current_commit_id_for_replica(
    session: &ScyllaSession,
    replica_id: ReplicaId,
) -> Result<Option<CommitId>> {
    let replica_table = session.table_name(REPLICA_TABLE_NAME);

    let raw_id = session
        .query(
            format!("SELECT commit_id FROM {replica_table} WHERE id = ?"),
            (replica_id.as_str(),),
        )
        .await?
        .rows_or_empty()
        .first()
        .and_then(|row| {
            row.columns[0]
                .as_ref()
                .and_then(|value| value.clone().into_string())
        });

    match raw_id {
        Some(id) => Ok(Some(Id::try_from(id)?)),
        None => Ok(None),
    }
}

async fn update_current_commit_id_for_replica(
    session: &ScyllaSession,
    replica_id: ReplicaId,
    commit_id: CommitId,
) -> Result<()> {
    let replica_table = session.table_name(REPLICA_TABLE_NAME);

    session
        .query(
            format!("INSERT INTO {replica_table} (id, commit_id) VALUES (?, ?)"),
            (replica_id.as_str(), commit_id.as_str()),
        )
        .await?;
    Ok(())
}

const COMMIT_TABLE_NAME: &str = "commit";
const OBJECT_TABLE_NAME: &str = "object";
const REF_TABLE_NAME: &str = "ref";
const REPLICA_TABLE_NAME: &str = "replica";

impl Store {
    pub async fn setup(hostname: impl Into<String>, keyspace: impl Into<String>) -> Result<Store> {
        let session = ScyllaSession::new(hostname, keyspace).await?;

        let ref_table_name = session.table_name(REF_TABLE_NAME);
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {ref_table_name}
                        (id BIGINT, left BIGINT, right BIGINT, object_ref BIGINT,
                        PRIMARY KEY (id))"
                ),
                &[],
            )
            .await
            .with_context(|| "Failed to create ref table")?;

        let object_table_name = session.table_name(OBJECT_TABLE_NAME);
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {object_table_name}
                        (id BIGINT, object BLOB,
                        PRIMARY KEY (id))"
                ),
                &[],
            )
            .await
            .with_context(|| "Failed to create object table")?;

        let commit_table_name = session.table_name(COMMIT_TABLE_NAME);
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {commit_table_name}
                        (id TEXT, version BLOB, root_ref BIGINT, prev_commit_id TEXT, PRIMARY KEY (id))"),
                &[],
            )
            .await
            .with_context(|| "Failed to create commit table")?;

        let replica_table_name = session.table_name(REPLICA_TABLE_NAME);
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {replica_table_name}
                        (id TEXT, commit_id TEXT, PRIMARY KEY (id))"
                ),
                &[],
            )
            .await
            .with_context(|| "Failed to create replica table")?;

        Ok(Self::Scylla(session))
    }

    #[cfg(test)]
    pub fn test() -> Self {
        Self::Fake {
            commits: RefCell::new(HashMap::new()),
            objects: RefCell::new(HashMap::new()),
            refs: RefCell::new(HashMap::new()),
            replicas: RefCell::new(HashMap::new()),
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
                        (id as i64,),
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

impl ObjectStore for Store {
    async fn resolve_object<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
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

                let data: T = ENCODING
                    .decode(object_blob.as_slice())
                    .with_context(|| "Failed to deserialize object")?;
                Ok(Some(data))
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

    async fn resolve_objects<T: Hash + DecodeOwned<Binary>>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<T>>> {
        match self {
            Store::Scylla(session) => {
                const MAX_CHUNK_SIZE: usize = 100;

                let object_table = session.table_name(OBJECT_TABLE_NAME);
                let query = format!("SELECT id, object FROM {object_table} WHERE id IN ?");
                let ids_i64: Vec<i64> = ids.iter().map(|&id| id as i64).collect();

                let mut result = std::iter::repeat_with(|| None)
                    .take(ids.len())
                    .collect::<Vec<_>>();

                for chunk in ids_i64.chunks(MAX_CHUNK_SIZE) {
                    let rows = session
                        .query(query.clone(), (chunk,))
                        .await?
                        .rows_or_empty();

                    for row in rows {
                        let id = row.columns[0]
                            .as_ref()
                            .and_then(|value| value.as_bigint())
                            .with_context(|| "Failed to deserialize id")?;
                        let object_blob = row.columns[1]
                            .as_ref()
                            .and_then(|value| value.clone().into_blob())
                            .with_context(|| "Failed to deserialize value")?;

                        let data: T = ENCODING
                            .decode(object_blob.as_slice())
                            .with_context(|| "Failed to deserialize object")?;

                        if let Some(index) = ids.iter().position(|&x| x == id as u64) {
                            result[index] = Some(data);
                        }
                    }
                }

                Ok(result)
            }
            #[cfg(test)]
            Store::Fake { objects, .. } => {
                let objects = objects.borrow();
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    if let Some(bytes) = objects.get(&id) {
                        let decoded = ENCODING
                            .decode(bytes.as_slice())
                            .with_context(|| "Failed to deserialize object")?;
                        result.push(Some(decoded));
                    } else {
                        result.push(None);
                    }
                }
                Ok(result)
            }
        }
    }

    async fn insert_objects<'a, T: 'a + Hash + Encode<Binary>>(
        &self,
        objects: &[&'a T],
    ) -> Result<Vec<ObjectRef>> {
        match self {
            Store::Scylla(session) => {
                let mut instant = None;
                if log_enabled!(log::Level::Debug) {
                    instant = Some(Instant::now());
                }

                let prepared: PreparedStatement = session
                    .raw()
                    .prepare(format!(
                        "INSERT INTO {} (id, object) VALUES (?, ?)",
                        session.table_name(OBJECT_TABLE_NAME)
                    ))
                    .await?;

                const BATCH_SIZE: usize = 2000;
                let total_objects = objects.len();
                let mut hashes = Vec::with_capacity(total_objects);

                for chunk in (0..total_objects).step_by(BATCH_SIZE) {
                    let end = std::cmp::min(chunk + BATCH_SIZE, total_objects);
                    let mut batch = Batch::default();
                    let mut values = Vec::with_capacity(end - chunk);

                    for object in &objects[chunk..end] {
                        batch.append_statement(prepared.clone());

                        let mut hasher = std::hash::DefaultHasher::new();
                        object.hash(&mut hasher);
                        let hash = hasher.finish();
                        hashes.push(hash);

                        let mut data = Vec::new();
                        ENCODING
                            .encode(&mut data, object)
                            .with_context(|| "Failed to serialize object")?;

                        values.push((hash as i64, data));
                    }

                    session.raw().batch(&batch, &values).await?;
                }

                if let Some(instant) = instant {
                    log::debug!(
                        "Inserting {} objects took {:?}",
                        total_objects,
                        instant.elapsed()
                    );
                }

                Ok(hashes)
            }
            #[cfg(test)]
            Store::Fake { .. } => {
                let mut hashes = Vec::new();
                for object in objects {
                    hashes.push(self.insert_object(object).await?);
                }
                Ok(hashes)
            }
        }
    }

    async fn insert_object<T: Hash + Encode<Binary>>(&self, object: &T) -> Result<u64> {
        match self {
            Store::Scylla(session) => {
                let mut instant = None;
                if log_enabled!(log::Level::Debug) {
                    instant = Some(Instant::now());
                }

                let mut data = Vec::new();
                ENCODING
                    .encode(&mut data, object)
                    .with_context(|| "Failed to serialize object")?;

                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                let hash = hasher.finish();

                session
                    .query(
                        format!(
                            "INSERT INTO {} (id, object) VALUES (?, ?)",
                            session.table_name(OBJECT_TABLE_NAME)
                        ),
                        (hash as i64, data),
                    )
                    .await?;

                if let Some(instant) = instant {
                    log::debug!("Inserting object took {:?}", instant.elapsed());
                }

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
}

impl VersionedStore for Store {
    async fn resolve_versioned<T: Deserialize + fmt::Debug>(&self, root: u64) -> Result<Option<T>> {
        let mut instant = None;
        if log_enabled!(log::Level::Debug) {
            instant = Some(Instant::now());
        }
        log::debug!("Resolving object with root id {root}");
        let Some(root) = self.resolve_ref(Some(root)).await? else {
            return Ok(None);
        };
        let cx = DeserializeCx { store: self };
        let result = T::deserialize(root, cx).await?;
        if log_enabled!(log::Level::Debug) {
            if let Some(elapsed) = instant.map(|i| i.elapsed()) {
                log::debug!("Resolving object took {} ms", elapsed.as_millis());
            }
        }
        Ok(Some(result))
    }

    async fn insert_versioned<T: Serialize + fmt::Debug>(&self, object: &T) -> Result<u64> {
        let mut insert_versioned_time = None;
        if log_enabled!(log::Level::Debug) {
            insert_versioned_time = Some(Instant::now());
        }
        let cx = SerializeCx { store: self };
        let references = object.serialize(cx).await?;
        log::debug!("Inserting object with {} references", references.len());
        let mut reference_time = None;
        if log_enabled!(log::Level::Debug) {
            reference_time = Some(Instant::now());
        }

        let root_ref_id = references.last().with_context(|| "Structure is empty")?.id;

        let root_ref = match self {
            Store::Scylla(session) => {
                let prepared: PreparedStatement = session
                    .raw()
                    .prepare(format!(
                        "INSERT INTO {} (id, left, right, object_ref) VALUES (?, ?, ?, ?)",
                        session.table_name(REF_TABLE_NAME)
                    ))
                    .await?;

                let mut batch = Batch::default();
                for _ in 0..references.len() {
                    batch.append_statement(prepared.clone());
                }
                let values = references
                    .into_iter()
                    .map(|reference| {
                        (
                            reference.id as i64,
                            reference.left.map(|id| id as i64),
                            reference.right.map(|id| id as i64),
                            reference.object_ref as i64,
                        )
                    })
                    .collect::<Vec<_>>();

                session.raw().batch(&batch, values).await?;

                root_ref_id
            }
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let mut refs = refs.borrow_mut();
                for reference in references {
                    refs.insert(reference.id, reference);
                }
                root_ref_id
            }
        };

        if log_enabled!(log::Level::Debug) {
            if let Some(elapsed) = reference_time.map(|i| i.elapsed()) {
                log::debug!("Inserting references took {} ms", elapsed.as_millis());
            }
            if let Some(elapsed) = insert_versioned_time.map(|i| i.elapsed()) {
                log::debug!("Inserting versioned object took {} ms", elapsed.as_millis());
            }
        }

        Ok(root_ref)
    }
}

pub struct SerializeCx<'a> {
    store: &'a Store,
}

impl SerializeCx<'_> {
    pub async fn insert_object<T: Hash + Encode<Binary>>(&self, object: &T) -> Result<u64> {
        self.store.insert_object(object).await
    }

    pub async fn insert_objects<'a, T: 'a + Hash + Encode<Binary>>(
        &self,
        objects: &[&'a T],
    ) -> Result<Vec<u64>> {
        self.store.insert_objects(objects).await
    }

    pub async fn serialize_iter<'a, T: 'a + Hash + Encode<Binary>>(
        &self,
        iter: impl Iterator<Item = &'a T>,
    ) -> Result<Vec<Ref>> {
        let mut prev_hash: Option<u64> = None;
        let objects = iter.collect::<Vec<_>>();
        let mut refs = Vec::with_capacity(objects.len());
        let object_refs = self.insert_objects(&objects).await?;
        for object_ref in object_refs {
            refs.push(Ref {
                id: object_ref,
                left: prev_hash,
                right: None,
                object_ref,
            });
            prev_hash = Some(object_ref);
        }
        Ok(refs)
    }
}

pub struct DeserializeCx<'a> {
    store: &'a Store,
}

impl DeserializeCx<'_> {
    pub async fn resolve_object<T: Hash + DecodeOwned<Binary>>(
        &self,
        id: u64,
    ) -> Result<Option<T>> {
        self.store.resolve_object(id).await
    }

    pub async fn resolve_objects<T: Hash + DecodeOwned<Binary>>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<T>>> {
        self.store.resolve_objects(ids).await
    }

    pub async fn resolve_ref(&self, id: Option<u64>) -> Result<Option<Ref>> {
        self.store.resolve_ref(id).await
    }

    pub async fn deserialize_iter<T: Hash + DecodeOwned<Binary>>(
        &self,
        root: Ref,
    ) -> Result<Vec<T>> {
        let mut items = Vec::new();
        items.push(root.object_ref);
        let mut node = root;
        while let Some(new_node) = self.resolve_ref(node.left).await? {
            items.push(new_node.object_ref);
            node = new_node
        }
        items.reverse();

        let objects = self.resolve_objects::<T>(&items).await?;
        let mut objects_only = Vec::with_capacity(objects.len());
        for object in objects {
            let Some(object) = object else {
                return Err(anyhow!("Object not found"));
            };
            objects_only.push(object);
        }
        Ok(objects_only)
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

// Some debugging tools, which can be helpful for benchmarks
impl Store {
    pub async fn dump_table_counts(&self) -> Result<()> {
        match self {
            Store::Scylla(session) => {
                async fn get_table_count(session: &ScyllaSession, table_name: &str) -> Result<i64> {
                    session
                        .query(
                            format!("SELECT COUNT(*) FROM {}", session.table_name(table_name)),
                            (),
                        )
                        .await?
                        .single_row()?
                        .columns[0]
                        .clone()
                        .with_context(|| "No entries")?
                        .as_bigint()
                        .with_context(|| "Invalid value")
                }

                let commit_count = get_table_count(session, "commit").await?;
                let object_count = get_table_count(session, "object").await?;
                let ref_count = get_table_count(session, "ref").await?;
                let replica_count = get_table_count(session, "replica").await?;
                log::debug!(
                    "Current amount of entities, commits: {}, objects: {}, refs: {}, replicas: {}",
                    commit_count,
                    object_count,
                    ref_count,
                    replica_count
                );
            }
            #[cfg(test)]
            Store::Fake {
                commits,
                objects,
                refs,
                replicas,
            } => {
                log::debug!(
                    "commit: {}, object: {}, ref: {}, replica: {}",
                    commits.borrow().len(),
                    objects.borrow().len(),
                    refs.borrow().len(),
                    replicas.borrow().len()
                );
            }
        }
        Ok(())
    }

    pub async fn reset_db(&self) -> Result<()> {
        match self {
            Store::Scylla(session) => {
                let table_names = [
                    COMMIT_TABLE_NAME,
                    OBJECT_TABLE_NAME,
                    REF_TABLE_NAME,
                    REPLICA_TABLE_NAME,
                ];

                for table_name in table_names {
                    let table = session.table_name(table_name);
                    session.query(format!("DROP TABLE {}", table), ()).await?;
                }
            }
            #[cfg(test)]
            Store::Fake {
                commits,
                objects,
                refs,
                replicas,
            } => {
                commits.borrow_mut().clear();
                objects.borrow_mut().clear();
                refs.borrow_mut().clear();
                replicas.borrow_mut().clear();
            }
        }
        Ok(())
    }
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

                let object_ref = cx.insert_object(object).await?;
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
                cx.resolve_object(root.object_ref)
                    .await?
                    .with_context(|| "Missing object")?,
            );

            let mut node = root;
            while let Some(new_node) = cx.resolve_ref(node.left).await? {
                items.push(
                    cx.resolve_object(new_node.object_ref)
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
