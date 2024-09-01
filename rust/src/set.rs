use super::*;

impl<T: MrdtItem> Mergeable for HashSet<T> {
    fn merge(lca: &Self, left: &Self, right: &Self) -> Self {
        let mut values = lca
            .intersection(left)
            .filter(|&item| right.contains(item))
            .cloned()
            .collect::<HashSet<_>>();

        values.extend(left.difference(lca).cloned());
        values.extend(right.difference(lca).cloned());

        values
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
    fn test_merge() {
        let id1 = Id::gen();
        let id2 = Id::gen();
        let id3 = Id::gen();
        let entity1 = TestEntity::new(id1);
        let entity2 = TestEntity::new(id2);
        let entity3 = TestEntity::new(id3);

        let mut set1 = HashSet::default();
        set1.insert(entity1.clone());
        set1.insert(entity2.clone());

        let mut set2 = HashSet::default();
        set2.insert(entity2.clone());
        set2.insert(entity3.clone());

        let mut lca = HashSet::default();
        lca.insert(entity2.clone());

        let merged = Mergeable::merge(&lca, &set1, &set2);
        assert!(merged.contains(&entity1));
        assert!(merged.contains(&entity2));
        assert!(merged.contains(&entity3));
    }
}
