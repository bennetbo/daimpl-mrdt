use super::*;
use ord::MrdtOrd;
use std::{fmt::Debug, ops::Index};

#[derive(Debug, Clone, Decode, Encode, Hash, PartialEq, Eq)]
pub struct MrdtList<T: MrdtItem> {
    pub(crate) mem: MrdtSet<T>,
    pub(crate) ord: MrdtOrd<T>,
}

impl<T: MrdtItem> Default for MrdtList<T> {
    fn default() -> Self {
        Self {
            mem: Default::default(),
            ord: Default::default(),
        }
    }
}

impl<T: MrdtItem> MrdtList<T> {
    /// Returns the number of elements in the list.
    pub fn len(&self) -> usize {
        self.mem.len()
    }

    /// Returns `true` if the list contains no elements.
    pub fn is_empty(&self) -> bool {
        self.mem.is_empty()
    }

    /// Returns an iterator over the elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.ord.iter()
    }

    /// Returns `true` if the list contains the specified value.
    pub fn contains(&self, value: &T) -> bool {
        self.mem.contains(value)
    }

    /// Returns the index of the specified value in the list.
    pub fn index_of(&self, value: &T) -> Option<usize> {
        self.ord.index_of(value)
    }

    /// Inserts a value into the set
    pub fn insert(&mut self, ix: usize, value: T) {
        self.mem.insert(value.clone());
        self.ord.insert(ix, value);
    }

    /// Adds a value to the end of the list
    pub fn add(&mut self, value: T) {
        let len = self.mem.len();
        self.mem.insert(value.clone());
        if len != self.mem.len() {
            self.ord.insert(len, value);
        }
    }

    /// Removes a value from the list
    pub fn remove(&mut self, value: &T) {
        let ix = self.ord.index_of(value);
        if let Some(ix) = ix {
            self.remove_at(ix);
        }
    }

    /// Removes the element at the specified index
    pub fn remove_at(&mut self, ix: usize) {
        if let Some(removed) = self.ord.remove_at(ix) {
            self.mem.remove(&removed);
        }
    }
}

impl<T: MrdtItem> Index<usize> for MrdtList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.iter().nth(index).unwrap()
    }
}

impl<T: Entity + MrdtItem> Entity for MrdtList<T> {
    fn table_name() -> &'static str {
        T::table_name()
    }
}

impl<T: MrdtItem + Ord> Mergeable<MrdtList<T>> for MrdtList<T> {
    fn merge(lca: &MrdtList<T>, left: &MrdtList<T>, right: &MrdtList<T>) -> MrdtList<T> {
        let mem = MrdtSet::merge(&lca.mem, &left.mem, &right.mem);
        let ord = MrdtOrd::merge(&lca.ord, &left.ord, &right.ord, &mem);
        Self { mem, ord }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    #[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
    struct TestItem {
        id: usize,
        value: String,
    }

    impl_entity!(TestItem, "test_items");

    #[test]
    fn test_list_empty() {
        let list: MrdtList<TestItem> = MrdtList::default();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_list_insert() {
        let mut list: MrdtList<TestItem> = MrdtList::default();
        let item1 = TestItem {
            id: 1,
            value: "Item 1".into(),
        };
        let item2 = TestItem {
            id: 2,
            value: "Item 2".into(),
        };
        let item3 = TestItem {
            id: 3,
            value: "Item 3".into(),
        };
        let item4 = TestItem {
            id: 4,
            value: "Item 4".into(),
        };

        list.add(item1.clone());
        list.add(item2.clone());
        list.add(item3.clone());
        list.insert(0, item4.clone());

        assert_eq!(list.len(), 4);
        assert_eq!(list.index_of(&item4), Some(0));
        assert_eq!(list.index_of(&item1), Some(1));
        assert_eq!(list.index_of(&item2), Some(2));
        assert_eq!(list.index_of(&item3), Some(3));
    }

    #[test]
    fn test_list_insert_duplicate() {
        let mut list: MrdtList<TestItem> = MrdtList::default();
        let item = TestItem {
            id: 1,
            value: "Item 1".into(),
        };

        list.insert(0, item.clone());
        list.insert(0, item.clone());

        assert_eq!(list.len(), 1);
        assert!(list.contains(&item));
    }

    #[test]
    fn test_list_remove() {
        let mut list: MrdtList<TestItem> = MrdtList::default();
        let item1 = TestItem {
            id: 1,
            value: "Item 1".into(),
        };
        let item2 = TestItem {
            id: 2,
            value: "Item 2".into(),
        };
        let item3 = TestItem {
            id: 3,
            value: "Item 3".into(),
        };

        list.add(item1.clone());
        list.add(item2.clone());
        list.add(item3.clone());

        assert_eq!(list.len(), 3);

        list.remove(&item1);

        assert_eq!(list.len(), 2);
        assert_eq!(list.index_of(&item2), Some(0));
        assert_eq!(list.index_of(&item3), Some(1));
    }

    #[test]
    fn test_list_merge_add() {
        let mut lca: MrdtList<TestItem> = MrdtList::default();
        let item1 = TestItem {
            id: 1,
            value: "Item 1".into(),
        };
        let item2 = TestItem {
            id: 2,
            value: "Item 2".into(),
        };
        let item3 = TestItem {
            id: 3,
            value: "Item 3".into(),
        };
        let item4 = TestItem {
            id: 4,
            value: "Item 4".into(),
        };
        let item5 = TestItem {
            id: 5,
            value: "Item 5".into(),
        };

        lca.add(item1.clone());
        lca.add(item2.clone());
        lca.add(item3.clone());

        let mut replica1 = lca.clone();
        let mut replica2 = lca.clone();

        replica1.add(item4.clone());
        replica2.remove(&item1);
        replica2.add(item5.clone());

        let merged_list = MrdtList::merge(&lca, &replica1, &replica2);
        assert_eq!(merged_list.len(), 4);
        assert_eq!(merged_list.index_of(&item2), Some(0));
        assert_eq!(merged_list.index_of(&item3), Some(1));
        assert_eq!(merged_list.index_of(&item4), Some(2));
        assert_eq!(merged_list.index_of(&item5), Some(3));
    }

    #[test]
    fn test_list_merge_insert() {
        let mut lca: MrdtList<TestItem> = MrdtList::default();
        let item1 = TestItem {
            id: 1,
            value: "Item 1".into(),
        };
        let item2 = TestItem {
            id: 2,
            value: "Item 2".into(),
        };
        let item3 = TestItem {
            id: 3,
            value: "Item 3".into(),
        };
        let item4 = TestItem {
            id: 4,
            value: "Item 4".into(),
        };
        let item5 = TestItem {
            id: 5,
            value: "Item 5".into(),
        };

        lca.add(item1.clone());
        lca.add(item2.clone());
        lca.add(item3.clone());

        let mut replica1 = lca.clone();
        let mut replica2 = lca.clone();

        replica1.insert(0, item4.clone());
        replica2.remove(&item1);
        replica2.insert(0, item5.clone());

        let merged_list = MrdtList::merge(&lca, &replica1, &replica2);
        assert_eq!(merged_list.len(), 4);
        assert_eq!(merged_list.index_of(&item4), Some(0));
        assert_eq!(merged_list.index_of(&item5), Some(1));
        assert_eq!(merged_list.index_of(&item2), Some(2));
        assert_eq!(merged_list.index_of(&item3), Some(3));
    }
}
