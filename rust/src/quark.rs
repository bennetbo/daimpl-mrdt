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
use std::time::Instant;

#[cfg(test)]
use std::{cell::RefCell, collections::HashMap};

use crate::{HashSet, Id, MrdtItem, ReplicaId, VectorClock};

const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

#[derive(Clone, Debug)]
pub struct Ref {
    pub id: u64,
    pub left: Option<u64>,
    pub right: Option<u64>,
    pub object_ref: u64,
}

impl Ref {
    pub fn compute(object_ref: u64, left: Option<u64>, right: Option<u64>) -> Self {
        let mut hasher = std::hash::DefaultHasher::new();
        object_ref.hash(&mut hasher);
        if let Some(left) = left {
            left.hash(&mut hasher);
        }
        if let Some(right) = right {
            right.hash(&mut hasher);
        }
        let id = hasher.finish();

        Self {
            id,
            left,
            right,
            object_ref,
        }
    }
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

pub enum QuarkStore {
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
pub trait VersionedStore {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit>;
    async fn commit(
        &self,
        replica_id: ReplicaId,
        version: VectorClock,
        root_ref: u64,
    ) -> Result<Commit>;

    async fn latest_commit_for_replica(&self, replica_id: ReplicaId) -> Result<Option<Commit>>;
    async fn resolve_commit(&self, commit_id: CommitId) -> Result<Commit>;
    async fn resolve_commit_for_version(&self, version: VectorClock) -> Result<Commit>;
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
pub trait RefStore {
    async fn resolve<T: Deserialize>(&self, root: u64) -> Result<Option<T>>;
    async fn insert<T: Serialize>(&self, object: &T) -> Result<u64>;
}

#[allow(async_fn_in_trait)]
pub trait Serialize {
    async fn serialize(&self, cx: SerializeCx) -> Result<Vec<Ref>>;
}

#[allow(async_fn_in_trait)]
pub trait Deserialize: Sized {
    async fn deserialize(root: Ref, cx: DeserializeCx) -> Result<Self>;
}

const BATCH_SIZE: usize = 2000;

impl VersionedStore for QuarkStore {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit> {
        log::debug!("Cloning replica {replica_id}");
        match self {
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake {
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
        log::debug!("Replica {replica_id} adding new commit. Ref: {root_ref}, Version: {version}");
        match self {
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake {
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
            QuarkStore::Scylla(session) => {
                match current_commit_id_for_replica(session, replica_id).await? {
                    Some(commit_id) => Ok(Some(self.resolve_commit(commit_id).await?)),
                    None => Ok(None),
                }
            }
            #[cfg(test)]
            QuarkStore::Fake {
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
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { commits, .. } => {
                let commits = commits.borrow();
                commits
                    .get(&commit_id)
                    .with_context(|| "Commit not found")
                    .cloned()
            }
        }
    }

    async fn resolve_commit_for_version(&self, version: VectorClock) -> Result<Commit> {
        match self {
            QuarkStore::Scylla(session) => {
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
                    .first_row()
                    .with_context(|| "Failed to select the first row")?;

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
            QuarkStore::Fake { commits, .. } => {
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

impl QuarkStore {
    pub async fn setup(
        hostname: impl Into<String>,
        keyspace: impl Into<String>,
    ) -> Result<QuarkStore> {
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
            QuarkStore::Scylla(_) => todo!(),
            #[cfg(test)]
            QuarkStore::Fake { refs, .. } => {
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
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { refs, .. } => {
                let refs = refs.borrow();
                Ok(refs.get(&id).cloned())
            }
        }
    }
}

impl ObjectStore for QuarkStore {
    async fn resolve_object<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
        match self {
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { objects, .. } => {
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
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { objects, .. } => {
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
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { .. } => {
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
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake { objects, .. } => {
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

impl RefStore for QuarkStore {
    async fn resolve<T: Deserialize>(&self, root: u64) -> Result<Option<T>> {
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

    async fn insert<T: Serialize>(&self, object: &T) -> Result<u64> {
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
            QuarkStore::Scylla(session) => {
                let prepared: PreparedStatement = session
                    .raw()
                    .prepare(format!(
                        "INSERT INTO {} (id, left, right, object_ref) VALUES (?, ?, ?, ?)",
                        session.table_name(REF_TABLE_NAME)
                    ))
                    .await?;

                let total_refs = references.len();
                for chunk in (0..total_refs).step_by(BATCH_SIZE) {
                    let end = std::cmp::min(chunk + BATCH_SIZE, total_refs);
                    let mut batch = Batch::default();
                    let mut values = Vec::with_capacity(end - chunk);

                    for reference in &references[chunk..end] {
                        batch.append_statement(prepared.clone());
                        values.push((
                            reference.id as i64,
                            reference.left.map(|id| id as i64),
                            reference.right.map(|id| id as i64),
                            reference.object_ref as i64,
                        ));
                    }

                    session.raw().batch(&batch, &values).await?;
                }

                root_ref_id
            }
            #[cfg(test)]
            QuarkStore::Fake { refs, .. } => {
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
    store: &'a QuarkStore,
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
            let reference = Ref::compute(object_ref, prev_hash, None);
            prev_hash = Some(reference.id);
            refs.push(reference);
        }
        Ok(refs)
    }
}

pub struct DeserializeCx<'a> {
    store: &'a QuarkStore,
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
                log::debug!("Object not found for ids: {:?}", &items);
                self.store.dump().await?;
                return Err(anyhow!("Object not found"));
            };
            objects_only.push(object);
        }
        Ok(objects_only)
    }
}

// Some debugging tools, which can be helpful for benchmarks

pub struct TableCounts {
    pub commits: u64,
    pub objects: u64,
    pub refs: u64,
    pub replicas: u64,
}

impl QuarkStore {
    pub async fn dump(&self) -> Result<()> {
        if !log_enabled!(log::Level::Debug) {
            return Ok(());
        }

        self.dump_table_counts().await?;
        self.dump_replicas().await?;
        self.dump_commits().await?;
        self.dump_refs().await?;
        self.dump_objects().await?;
        Ok(())
    }

    async fn dump_replicas(&self) -> Result<()> {
        log::debug!("---------- Dumping replicas... ----------");
        match self {
            QuarkStore::Scylla(session) => {
                for row in session
                    .query(
                        format!(
                            "SELECT id, commit_id FROM {}",
                            session.table_name(REPLICA_TABLE_NAME)
                        ),
                        (),
                    )
                    .await?
                    .rows()?
                {
                    let id = row.columns[0]
                        .as_ref()
                        .and_then(|value| value.clone().into_string())
                        .with_context(|| "Failed to deserialize value")?;
                    let latest_commit_id = row.columns[1]
                        .as_ref()
                        .and_then(|value| value.clone().into_string());

                    log::debug!("{} {:?}", id, latest_commit_id);
                }
            }
            #[cfg(test)]
            QuarkStore::Fake { replicas, .. } => {
                let replicas = replicas.borrow();
                for (id, commit) in replicas.iter() {
                    log::debug!("{} {}", id, commit);
                }
            }
        }
        Ok(())
    }

    async fn dump_commits(&self) -> Result<()> {
        log::debug!("---------- Dumping commits... ----------");
        match self {
            QuarkStore::Scylla(session) => {
                for row in session
                    .query(
                        format!(
                            "SELECT id, root_ref, version, prev_commit_id FROM {}",
                            session.table_name(COMMIT_TABLE_NAME)
                        ),
                        (),
                    )
                    .await?
                    .rows()?
                {
                    let id = row.columns[0]
                        .as_ref()
                        .and_then(|value| value.clone().into_string())
                        .with_context(|| "Failed to deserialize value")?;
                    let root_ref = row.columns[1]
                        .as_ref()
                        .and_then(|value| value.as_bigint())
                        .with_context(|| "Failed to deserialize root ref")?;
                    let version_blob = row.columns[2]
                        .as_ref()
                        .and_then(|value| value.clone().into_blob())
                        .with_context(|| "Failed to deserialize value")?;
                    let version: VectorClock = ENCODING
                        .decode(version_blob.as_slice())
                        .with_context(|| "Failed to serialize version")?;
                    let prev_commit_id = row.columns[3]
                        .as_ref()
                        .and_then(|value| value.clone().into_string());

                    log::debug!(
                        "ID: {} Root Ref: {} Version: {} Prev Commit ID: {:?}",
                        id,
                        root_ref as u64,
                        version,
                        prev_commit_id
                    );
                }
            }
            #[cfg(test)]
            QuarkStore::Fake { commits, .. } => {
                let commits = commits.borrow();
                for (id, commit) in commits.iter() {
                    log::debug!("{} {:?} {:?}", id, commit.version, commit.parent_commit_id);
                }
            }
        }
        Ok(())
    }

    async fn dump_refs(&self) -> Result<()> {
        log::debug!("---------- Dumping refs... ----------");
        match self {
            QuarkStore::Scylla(session) => {
                for row in session
                    .query(
                        format!(
                            "SELECT id, left, right, object_ref FROM {}",
                            session.table_name(REF_TABLE_NAME)
                        ),
                        (),
                    )
                    .await?
                    .rows()?
                {
                    let id = row.columns[0]
                        .as_ref()
                        .and_then(|value| value.as_bigint())
                        .with_context(|| "Failed to deserialize id")?;
                    let left = row.columns[1].as_ref().and_then(|value| value.as_bigint());
                    let right = row.columns[2].as_ref().and_then(|value| value.as_bigint());
                    let object_ref = row.columns[3]
                        .as_ref()
                        .and_then(|value| value.as_bigint())
                        .with_context(|| "Failed to deserialize object_ref")?;

                    log::debug!(
                        "ID: {}, Left: {:?}, Right: {:?}, Object Ref: {}",
                        id as u64,
                        left.map(|id| id as u64),
                        right.map(|id| id as u64),
                        object_ref as u64
                    );
                }
            }
            #[cfg(test)]
            QuarkStore::Fake { refs, .. } => {
                let refs = refs.borrow();
                for (id, ref_) in refs.iter() {
                    log::debug!(
                        "ID: {}, Left: {:?}, Right: {:?}, Object Ref: {}",
                        id,
                        ref_.left,
                        ref_.right,
                        ref_.object_ref
                    );
                }
            }
        }
        Ok(())
    }

    async fn dump_objects(&self) -> Result<()> {
        log::debug!("---------- Dumping objects... ----------");
        match self {
            QuarkStore::Scylla(session) => {
                for row in session
                    .query(
                        format!("SELECT id FROM {}", session.table_name(OBJECT_TABLE_NAME)),
                        (),
                    )
                    .await?
                    .rows()?
                {
                    let id = row.columns[0]
                        .as_ref()
                        .and_then(|value| value.as_bigint())
                        .with_context(|| "Failed to deserialize id")?;

                    log::debug!("{}", id as u64);
                }
            }
            #[cfg(test)]
            QuarkStore::Fake { objects, .. } => {
                let objects = objects.borrow();
                for (id, _) in objects.iter() {
                    log::debug!("{}", id);
                }
            }
        }
        Ok(())
    }

    pub async fn table_counts(&self) -> Result<TableCounts> {
        let table_counts = match self {
            QuarkStore::Scylla(session) => {
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

                let commits = get_table_count(session, "commit").await? as u64;
                let objects = get_table_count(session, "object").await? as u64;
                let refs = get_table_count(session, "ref").await? as u64;
                let replicas = get_table_count(session, "replica").await? as u64;
                TableCounts {
                    commits,
                    objects,
                    refs,
                    replicas,
                }
            }
            #[cfg(test)]
            QuarkStore::Fake {
                commits,
                objects,
                refs,
                replicas,
            } => TableCounts {
                commits: commits.borrow().len() as u64,
                objects: objects.borrow().len() as u64,
                refs: refs.borrow().len() as u64,
                replicas: replicas.borrow().len() as u64,
            },
        };

        Ok(table_counts)
    }

    pub async fn dump_table_counts(&self) -> Result<()> {
        if !log_enabled!(log::Level::Debug) {
            return Ok(());
        }

        let table_counts = self.table_counts().await?;
        log::debug!(
            "Current amount of entities, commits: {}, objects: {}, refs: {}, replicas: {}",
            table_counts.commits,
            table_counts.objects,
            table_counts.refs,
            table_counts.replicas
        );
        Ok(())
    }

    pub async fn reset_db(&self) -> Result<()> {
        match self {
            QuarkStore::Scylla(session) => {
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
            QuarkStore::Fake {
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

impl<T: MrdtItem> Serialize for HashSet<T> {
    async fn serialize(&self, cx: SerializeCx<'_>) -> Result<Vec<Ref>> {
        cx.serialize_iter(self.iter()).await
    }
}

impl<T: MrdtItem> Deserialize for HashSet<T> {
    async fn deserialize(root: Ref, cx: DeserializeCx<'_>) -> Result<Self> {
        let items = cx.deserialize_iter(root).await?;
        Ok(items.into_iter().collect())
    }
}

impl<T: MrdtItem> Serialize for Vec<T> {
    async fn serialize(&self, cx: SerializeCx<'_>) -> Result<Vec<Ref>> {
        cx.serialize_iter(self.iter()).await
    }
}

impl<T: MrdtItem> Deserialize for Vec<T> {
    async fn deserialize(root: Ref, cx: DeserializeCx<'_>) -> Result<Self> {
        let items = cx.deserialize_iter(root).await?;
        Ok(items.into_iter().collect())
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

        let store = QuarkStore::test();

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
