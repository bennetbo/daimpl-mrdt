use super::*;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash};

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct MrdtSet<T: MrdtItem> {
    pub(crate) store: im::HashSet<T>,
}

impl<T: MrdtItem> Default for MrdtSet<T> {
    fn default() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl<T: MrdtItem + Debug> Debug for MrdtSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.store.iter()).finish()
    }
}

impl<T: MrdtItem> MrdtSet<T> {
    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Returns an iterator over the elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.store.iter()
    }

    /// Returns `true` if the set contains the specified value.
    pub fn contains(&self, value: &T) -> bool {
        self.store.contains(value)
    }

    /// Inserts a value into the set, returning a new set with the value added.
    pub fn insert(&self, value: T) -> Self {
        Self {
            store: self.store.update(value),
        }
    }

    /// Inserts a value into the set
    pub fn insert_in_place(&mut self, value: T) {
        self.store.insert(value);
    }

    /// Removes a value from the set, returning a new set with the value removed.
    pub fn remove(&self, value: &T) -> Self {
        Self {
            store: self.store.without(value),
        }
    }

    /// Remove a value into the set
    pub fn remove_in_place(&mut self, value: &T) {
        self.store.remove(value);
    }
}

impl<T: MrdtItem> Mergeable<MrdtSet<T>> for MrdtSet<T> {
    fn merge(lca: &MrdtSet<T>, left: &MrdtSet<T>, right: &MrdtSet<T>) -> MrdtSet<T> {
        merge_sets(lca, left, right)
    }
}

pub fn merge_sets<T: MrdtItem>(
    lca: &MrdtSet<T>,
    left: &MrdtSet<T>,
    right: &MrdtSet<T>,
) -> MrdtSet<T> {
    let mut values = Vec::with_capacity(lca.len());

    for value in lca.iter() {
        if left.contains(value) && right.contains(value) {
            values.push(value.clone());
        }
    }

    for value in left.iter() {
        if !lca.contains(value) {
            values.push(value.clone());
        }
    }

    for value in right.iter() {
        if !lca.contains(value) {
            values.push(value.clone());
        }
    }

    MrdtSet {
        store: im::HashSet::from(values),
    }
}

impl<T: MrdtItem + Entity> Entity for MrdtSet<T> {
    fn table_name() -> &'static str {
        T::table_name()
    }
}

impl<T: MrdtItem> From<im::HashSet<T>> for MrdtSet<T> {
    fn from(value: im::HashSet<T>) -> Self {
        Self { store: value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
    struct TestEntity {
        id: Id,
    }

    impl TestEntity {
        fn new(id: Id) -> Self {
            TestEntity { id }
        }
    }

    impl Entity for TestEntity {
        fn table_name() -> &'static str {
            "test_entities"
        }
    }

    #[test]
    fn test_insert() {
        let set = MrdtSet::default();
        let id = Id::gen();
        let entity = TestEntity::new(id);

        let set = set.insert(entity.clone());

        assert_eq!(set.len(), 1);
        assert!(set.contains(&entity));
    }

    #[test]
    fn test_remove() {
        let set = MrdtSet::default();
        let id = Id::gen();
        let entity = TestEntity::new(id);

        let set = set.insert(entity.clone());
        let set = set.remove(&entity);

        assert!(set.is_empty());
        assert!(!set.contains(&entity));
    }

    #[test]
    fn test_len_and_is_empty() {
        let set = MrdtSet::default();
        assert!(set.is_empty());

        let id = Id::gen();
        let entity = TestEntity::new(id);

        let set = set.insert(entity.clone());
        assert_eq!(set.len(), 1);
        assert!(!set.is_empty());
    }

    #[test]
    fn test_iter() {
        let set = MrdtSet::default();
        let id1 = Id::gen();
        let id2 = Id::gen();
        let entity1 = TestEntity::new(id1);
        let entity2 = TestEntity::new(id2);

        let set = set.insert(entity1.clone());
        let set = set.insert(entity2.clone());

        let mut iter = set.iter();
        let item1 = iter.next().unwrap();
        let item2 = iter.next().unwrap();

        assert!(iter.next().is_none());
        assert!(set.contains(item1));
        assert!(set.contains(item2));
    }

    #[test]
    fn test_merge() {
        let id1 = Id::gen();
        let id2 = Id::gen();
        let id3 = Id::gen();
        let entity1 = TestEntity::new(id1);
        let entity2 = TestEntity::new(id2);
        let entity3 = TestEntity::new(id3);

        let set1 = MrdtSet::default()
            .insert(entity1.clone())
            .insert(entity2.clone());
        let set2 = MrdtSet::default()
            .insert(entity2.clone())
            .insert(entity3.clone());
        let lca = MrdtSet::default().insert(entity2.clone());

        let merged = MrdtSet::merge(&lca, &set1, &set2);
        assert!(merged.contains(&entity1));
        assert!(merged.contains(&entity2));
        assert!(merged.contains(&entity3));
    }
}
