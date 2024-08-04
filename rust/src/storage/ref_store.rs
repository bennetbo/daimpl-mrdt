use object_store::ObjectRef;
use replica::ReplicaId;

use super::*;

pub type CommitId = Id;

#[derive(Debug, Clone)]
pub struct Commit {
    pub id: CommitId,
    pub version: VectorClock,
    pub object_ref: ObjectRef,
    pub parent_commit_id: Option<Id>,
}

const COMMIT_TABLE_NAME: &str = "commit";
const REPLICA_TABLE_NAME: &str = "replica";

pub async fn create_tables(store: &mut Store) -> Result<()> {
    let commit_table_name = store.table_name(COMMIT_TABLE_NAME);
    let replica_table_name = store.table_name(REPLICA_TABLE_NAME);

    store.query(format!("CREATE TABLE IF NOT EXISTS {commit_table_name} (id TEXT, version BLOB, object_ref TEXT, prev_commit_id TEXT, PRIMARY KEY (id))"), &[]).await?;
    store
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {replica_table_name} (id TEXT, commit_id TEXT, PRIMARY KEY (id))"
            ),
            &[],
        )
        .await?;
    Ok(())
}

// Clone inserts a new replica id and stores which commit it points to
// Commit inserts a new commit id which points to a specific object (and the previous commit)
// After merge point replica_a commit id to the commit on the "main replica"

#[allow(async_fn_in_trait)]
pub trait RefStore {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit>;
    async fn commit(
        &self,
        replica_id: ReplicaId,
        version: VectorClock,
        object_ref: ObjectRef,
    ) -> Result<Commit>;

    async fn latest_commit_for_replica(&self, replica_id: ReplicaId) -> Result<Option<Commit>>;
    async fn resolve_commit(&self, commit_id: CommitId) -> Result<Commit>;
    async fn resolve_commit_by_version(&self, version: VectorClock) -> Result<Commit>;
}

impl RefStore for Store {
    async fn clone(&self, replica_id: ReplicaId) -> Result<Commit> {
        let commit_table = self.table_name(COMMIT_TABLE_NAME);

        let commit_id = self
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
        update_current_commit_id_for_replica(self, replica_id, commit_id).await?;
        Ok(commit)
    }

    async fn commit(
        &self,
        replica_id: ReplicaId,
        version: VectorClock,
        object_ref: ObjectRef,
    ) -> Result<Commit> {
        let commit_table = self.table_name(COMMIT_TABLE_NAME);

        let commit_id = Id::gen();

        let prev_commit_id = current_commit_id_for_replica(self, replica_id).await?;

        update_current_commit_id_for_replica(self, replica_id, commit_id).await?;

        let mut version_bytes = Vec::new();
        ENCODING
            .encode(&mut version_bytes, &version)
            .with_context(|| "Failed to serialize version")?;

        self.query(
            format!("INSERT INTO {commit_table} (id, version, object_ref, prev_commit_id) VALUES (?, ?, ?, ?)"),
            (
                commit_id.as_str(),
                &version_bytes,
                object_ref.to_string().as_str(),
                prev_commit_id.map(|id| id.to_string()),
            ),
        )
        .await?;

        Ok(Commit {
            id: commit_id,
            version,
            object_ref,
            parent_commit_id: prev_commit_id,
        })
    }

    async fn latest_commit_for_replica(&self, replica_id: ReplicaId) -> Result<Option<Commit>> {
        match current_commit_id_for_replica(self, replica_id).await? {
            Some(commit_id) => Ok(Some(self.resolve_commit(commit_id).await?)),
            None => Ok(None),
        }
    }

    async fn resolve_commit(&self, commit_id: CommitId) -> Result<Commit> {
        let commit_table = self.table_name(COMMIT_TABLE_NAME);

        let result = self
            .query(
                format!(
                    "SELECT version, object_ref, prev_commit_id FROM {commit_table} WHERE id = ?"
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

        let object_ref = result.columns[1]
            .as_ref()
            .and_then(|value| value.clone().into_string())
            .with_context(|| "Failed to deserialize object ref")?
            .parse::<u64>()?;

        let prev_commit_id = result.columns[2]
            .as_ref()
            .and_then(|value| value.clone().into_string());

        Ok(Commit {
            id: commit_id,
            version: ENCODING.decode(version_blob.as_slice())?,
            object_ref,
            parent_commit_id: prev_commit_id.and_then(|id| Id::try_from(id).ok()),
        })
    }

    async fn resolve_commit_by_version(&self, version: VectorClock) -> Result<Commit> {
        let commit_table = self.table_name(COMMIT_TABLE_NAME);

        let mut version_bytes = Vec::new();
        ENCODING.encode(&mut version_bytes, &version)?;
        let result = self
            .query(
                format!(
                    "SELECT id, object_ref, prev_commit_id FROM {commit_table} WHERE version = ? ALLOW FILTERING"
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

        let object_ref = result.columns[1]
            .as_ref()
            .and_then(|value| value.clone().into_string())
            .with_context(|| "Failed to deserialize value")?
            .parse::<u64>()?;

        let prev_commit_id = result.columns[2]
            .as_ref()
            .and_then(|value| value.clone().into_string());

        Ok(Commit {
            id: Id::try_from(commit_id)?,
            version,
            object_ref,
            parent_commit_id: prev_commit_id.and_then(|id| Id::try_from(id).ok()),
        })
    }
}

async fn current_commit_id_for_replica(
    store: &Store,
    replica_id: ReplicaId,
) -> Result<Option<CommitId>> {
    let replica_table = store.table_name(REPLICA_TABLE_NAME);

    let raw_id = store
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
    store: &Store,
    replica_id: ReplicaId,
    commit_id: CommitId,
) -> Result<()> {
    let replica_table = store.table_name(REPLICA_TABLE_NAME);

    store
        .query(
            format!("INSERT INTO {replica_table} (id, commit_id) VALUES (?, ?)"),
            (replica_id.as_str(), commit_id.as_str()),
        )
        .await?;
    Ok(())
}
