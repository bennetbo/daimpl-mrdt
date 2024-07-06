use list::MrdtList;

use super::*;

#[derive(Debug, Clone, Decode, Encode, Hash, PartialEq, Eq)]
pub struct MrdtQueue<T: MrdtItem> {
    pub(crate) store: MrdtList<T>,
}

impl<T: MrdtItem> Default for MrdtQueue<T> {
    fn default() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl<T: MrdtItem> MrdtQueue<T> {
    /// Returns the number of elements in the queue.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` if the queue contains no elements.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Returns an iterator over the elements in the queue.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.store.iter()
    }

    /// Returns `true` if the queue contains the specified value.
    pub fn contains(&self, value: &T) -> bool {
        self.store.contains(value)
    }

    /// Returns the index of the specified value in the queue.
    pub fn index_of(&self, value: &T) -> Option<usize> {
        self.store.index_of(value)
    }

    /// Inserts a value into the queue
    pub fn push(&mut self, value: T) {
        self.store.add(value);
    }

    /// Removes a value from the queue
    pub fn pop(&mut self) -> Option<T> {
        let element = self.store.iter().next().cloned();
        self.store.remove_at(0);
        element
    }
}

impl<T: MrdtItem + Ord> Mergeable<MrdtQueue<T>> for MrdtQueue<T> {
    fn merge(lca: &MrdtQueue<T>, left: &MrdtQueue<T>, right: &MrdtQueue<T>) -> MrdtQueue<T> {
        let store = MrdtList::merge(&lca.store, &left.store, &right.store);
        Self { store }
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

    #[test]
    fn test_queue_empty() {
        let queue: MrdtQueue<TestItem> = MrdtQueue::default();
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_queue_push() {
        let mut queue: MrdtQueue<TestItem> = MrdtQueue::default();
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

        queue.push(item1.clone());
        queue.push(item2.clone());
        queue.push(item3.clone());

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.index_of(&item1), Some(0));
        assert_eq!(queue.index_of(&item2), Some(1));
        assert_eq!(queue.index_of(&item3), Some(2));
    }

    #[test]
    fn test_queue_pop() {
        let mut queue: MrdtQueue<TestItem> = MrdtQueue::default();
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

        queue.push(item1.clone());
        queue.push(item2.clone());
        queue.push(item3.clone());

        assert_eq!(queue.len(), 3);
        let popped = queue.pop();
        assert_eq!(queue.len(), 2);
        assert_eq!(popped, Some(item1));
        assert_eq!(queue.index_of(&item2), Some(0));
        assert_eq!(queue.index_of(&item3), Some(1));
    }

    #[test]
    fn test_queue_merge() {
        let mut lca: MrdtQueue<TestItem> = MrdtQueue::default();
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

        lca.push(item1.clone());
        lca.push(item2.clone());
        lca.push(item3.clone());

        let mut replica1 = lca.clone();
        replica1.push(item4.clone());

        let mut replica2 = lca.clone();
        replica2.pop();
        replica2.push(item5.clone());

        let merged_queue = MrdtQueue::merge(&lca, &replica1, &replica2);
        assert_eq!(merged_queue.len(), 4);
        assert_eq!(merged_queue.index_of(&item2), Some(0));
        assert_eq!(merged_queue.index_of(&item3), Some(1));
        assert_eq!(merged_queue.index_of(&item4), Some(2));
        assert_eq!(merged_queue.index_of(&item5), Some(3));
    }
}
