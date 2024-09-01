use std::fmt;

use super::*;

pub type ReplicaId = Id;

pub struct Replica {
    id: ReplicaId,
    store: Store,
    latest_commit: Commit,
}

impl Replica {
    pub async fn clone(id: ReplicaId, store: Store) -> Result<Self> {
        let latest_commit = store.clone(id).await?;
        Ok(Self {
            id,
            store,
            latest_commit,
        })
    }

    /// Returns the underlying store of the replica.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Returns the identifier of the replica.
    pub fn id(&self) -> ReplicaId {
        self.id
    }

    /// Returns a reference to the latest commit of the replica.
    pub fn latest_commit(&self) -> &Commit {
        &self.latest_commit
    }

    /// Returns a reference to the latest version of the replica.
    pub fn latest_version(&self) -> &VectorClock {
        &self.latest_commit.version
    }

    /// Resolves and returns the object of the lastest commit from the store.
    pub async fn latest_object<T: Deserialize + fmt::Debug>(&self) -> Result<Option<T>> {
        self.store.resolve(self.latest_commit.root_ref).await
    }

    /// Commits the given object to the store and returns the resulting commit.
    pub async fn commit_object<T: Serialize + fmt::Debug>(&mut self, object: &T) -> Result<Commit> {
        let object_ref = self.store.insert(object).await?;
        self.commit(object_ref, self.next_version()).await
    }

    /// Commits the given object reference to the store and returns the resulting commit.
    pub async fn commit(&mut self, object_ref: ObjectRef, version: VectorClock) -> Result<Commit> {
        let commit = self.store.commit(self.id, version, object_ref).await?;
        self.latest_commit = commit.clone();
        Ok(commit)
    }

    /// Merges the current replica's state with another replica and commits the merged object.
    pub async fn merge_with<T: Serialize + Deserialize + Mergeable + fmt::Debug>(
        &mut self,
        other_replica: ReplicaId,
    ) -> Result<(Commit, T)> {
        let commit_to_merge_with = self
            .store
            .latest_commit_for_replica(other_replica)
            .await?
            .with_context(|| "Replica has no commits")?;

        let other_replica_version = commit_to_merge_with.version;

        let current_object = self
            .latest_object::<T>()
            .await?
            .with_context(|| "Empty object")?;
        let object_to_merge_with = self
            .store
            .resolve::<T>(commit_to_merge_with.root_ref)
            .await?
            .with_context(|| "LCA object is empty")?;

        let lca = VectorClock::lca(self.latest_version(), &other_replica_version);
        let lca_commit = self.store.resolve_commit_by_version(lca.clone()).await?;
        let lca_object = self
            .store
            .resolve::<T>(lca_commit.root_ref)
            .await?
            .with_context(|| "LCA object is empty")?;

        let merged_object = T::merge(&lca_object, &current_object, &object_to_merge_with);

        let object_ref = self.store.insert(&merged_object).await?;
        let version = VectorClock::merge(self.latest_version(), &other_replica_version);
        let commit = self.commit(object_ref, version).await?;

        Ok((commit, merged_object))
    }

    fn next_version(&self) -> VectorClock {
        let mut version = self.latest_commit.version.clone();
        version.inc(self.id);
        version
    }
}
