use musli::{Decode, Encode};

use super::*;
use std::{fmt::Debug, hash::Hash};

#[derive(Clone, PartialEq, Eq, Encode, Decode)]
pub struct MrdtSet<T: MrdtItem> {
    pub(crate) store: fxhash::FxHashSet<T>,
}

impl<T: MrdtItem> Hash for MrdtSet<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.store.len() as u64);
        for item in self.store.iter() {
            item.hash(state);
        }
    }
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

    /// Inserts a value into the set
    pub fn insert(&mut self, value: T) {
        self.store.insert(value);
    }

    /// Remove a value from the set
    pub fn remove(&mut self, value: &T) {
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
    let mut values = lca
        .store
        .intersection(&left.store)
        .filter(|&item| right.store.contains(item))
        .cloned()
        .collect::<fxhash::FxHashSet<_>>();

    values.extend(left.store.difference(&lca.store).cloned());
    values.extend(right.store.difference(&lca.store).cloned());

    MrdtSet { store: values }
}

impl<T: MrdtItem> From<fxhash::FxHashSet<T>> for MrdtSet<T> {
    fn from(value: fxhash::FxHashSet<T>) -> Self {
        Self { store: value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Encode, Decode, Debug)]
    struct TestEntity {
        id: Id,
    }

    impl TestEntity {
        fn new(id: Id) -> Self {
            TestEntity { id }
        }
    }

    #[test]
    fn test_insert() {
        let mut set = MrdtSet::default();
        let id = Id::gen();
        let entity = TestEntity::new(id);

        set.insert(entity.clone());

        assert_eq!(set.len(), 1);
        assert!(set.contains(&entity));
    }

    #[test]
    fn test_remove() {
        let mut set = MrdtSet::default();
        let id = Id::gen();
        let entity = TestEntity::new(id);

        set.insert(entity.clone());
        set.remove(&entity);

        assert!(set.is_empty());
        assert!(!set.contains(&entity));
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut set = MrdtSet::default();
        assert!(set.is_empty());

        let id = Id::gen();
        let entity = TestEntity::new(id);

        set.insert(entity.clone());

        assert_eq!(set.len(), 1);
        assert!(!set.is_empty());
    }

    #[test]
    fn test_iter() {
        let mut set = MrdtSet::default();
        let id1 = Id::gen();
        let id2 = Id::gen();
        let entity1 = TestEntity::new(id1);
        let entity2 = TestEntity::new(id2);

        set.insert(entity1.clone());
        set.insert(entity2.clone());

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

        let mut set1 = MrdtSet::default();
        set1.insert(entity1.clone());
        set1.insert(entity2.clone());

        let mut set2 = MrdtSet::default();
        set2.insert(entity2.clone());
        set2.insert(entity3.clone());

        let mut lca = MrdtSet::default();
        lca.insert(entity2.clone());

        let merged = MrdtSet::merge(&lca, &set1, &set2);
        assert!(merged.contains(&entity1));
        assert!(merged.contains(&entity2));
        assert!(merged.contains(&entity3));
    }
}
