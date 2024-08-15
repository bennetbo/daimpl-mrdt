use itertools::Itertools;

use super::*;
use std::{fmt::Debug, hash::Hash};

#[derive(Clone, Decode, Encode, PartialEq, Eq)]
pub struct MrdtOrd<T: MrdtItem> {
    pub(crate) store: fxhash::FxHashMap<T, usize>,
}

impl<T: MrdtItem> Hash for MrdtOrd<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.store.len() as u64);
        for item in self.store.iter() {
            item.hash(state);
        }
    }
}

impl<T: MrdtItem + Debug> Debug for MrdtOrd<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.store.iter()).finish()
    }
}

impl<T: MrdtItem> MrdtOrd<T> {
    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn index_of(&self, value: &T) -> Option<usize> {
        self.store.get(value).cloned()
    }

    pub fn contains(&self, value: &T) -> bool {
        self.store.contains_key(value)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.store.iter().sorted_by_key(|(_, v)| *v).map(|(k, _)| k)
    }

    pub fn insert(&mut self, ix: usize, value: T) {
        for (_, v) in self.store.iter_mut() {
            if *v >= ix {
                *v += 1;
            }
        }
        self.store.insert(value, ix);
    }

    pub fn remove(&mut self, value: &T) {
        self.store.remove(value);
    }

    pub fn remove_at(&mut self, ix: usize) -> Option<T> {
        let element = self
            .store
            .iter()
            .find(|(_, i)| **i == ix)
            .map(|(k, _)| k.clone());
        if let Some(element) = element {
            self.store.remove(&element);
            for (_, v) in self.store.iter_mut() {
                if *v > ix {
                    *v -= 1;
                }
            }
            Some(element)
        } else {
            None
        }
    }
}

pub fn toposort<T: MrdtItem + Ord>(pairs: &MrdtSet<(T, T)>) -> Vec<T> {
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashMap, HashSet};

    let mut graph: HashMap<T, Vec<T>> = HashMap::new();
    let mut in_degree: HashMap<T, usize> = HashMap::new();
    let mut all_nodes: HashSet<T> = HashSet::new();

    // Build the graph and calculate in-degrees
    for (from, to) in pairs.iter() {
        graph.entry(from.clone()).or_default().push(to.clone());
        *in_degree.entry(to.clone()).or_insert(0) += 1;
        all_nodes.insert(from.clone());
        all_nodes.insert(to.clone());
    }

    // Use a BinaryHeap as a priority queue
    // Reverse is used to make it a min-heap based on (in-degree, node value)
    let mut queue: BinaryHeap<Reverse<(usize, T)>> = all_nodes
        .into_iter()
        .map(|node| Reverse((*in_degree.get(&node).unwrap_or(&0), node)))
        .collect();

    let mut result = Vec::new();
    let mut processed = HashSet::new();

    while let Some(Reverse((_, node))) = queue.pop() {
        if processed.contains(&node) {
            continue;
        }

        result.push(node.clone());
        processed.insert(node.clone());

        if let Some(neighbors) = graph.get(&node) {
            for neighbor in neighbors {
                if processed.contains(neighbor) {
                    continue;
                }

                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    // Update the priority queue with the new in-degree
                    queue.push(Reverse((*degree, neighbor.clone())));
                }
            }
        }
    }

    result
}

impl<T: MrdtItem + Ord> MrdtOrd<T> {
    pub fn merge(lca: &Self, left: &Self, right: &Self, merged_mem: &MrdtSet<T>) -> Self {
        let left = map_to_ordering(&left.store);
        let right = map_to_ordering(&right.store);
        let lca = map_to_ordering(&lca.store);

        let union = left.union(&right.union(&lca));
        let set = toposort(&union);

        let mut merged_set = fxhash::FxHashMap::default();
        for value in set.iter() {
            if merged_mem.contains(value) {
                merged_set.insert(value.clone(), merged_set.len());
            }
        }
        Self { store: merged_set }
    }
}

impl<T: MrdtItem> Default for MrdtOrd<T> {
    fn default() -> Self {
        Self {
            store: fxhash::FxHashMap::default(),
        }
    }
}

fn map_to_ordering<T: MrdtItem>(ordering: &fxhash::FxHashMap<T, usize>) -> MrdtSet<(T, T)> {
    let mut ordered_set = fxhash::FxHashSet::default();
    let mut sorted_items: Vec<_> = ordering.iter().collect();
    sorted_items.sort_by_key(|(_, &idx)| idx);

    for (i, (value, _)) in sorted_items.iter().enumerate().skip(1) {
        let prev_value = sorted_items[i - 1].0;
        ordered_set.insert((prev_value.clone(), (*value).clone()));
    }

    MrdtSet::from(ordered_set)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_to_ordering() {
        let mut ordering = fxhash::FxHashMap::default();
        ordering.insert('a', 0);
        ordering.insert('b', 1);
        ordering.insert('c', 2);

        let result = map_to_ordering(&ordering);

        let expected: MrdtSet<(char, char)> = MrdtSet::from(
            vec![('a', 'b'), ('b', 'c')]
                .into_iter()
                .collect::<fxhash::FxHashSet<_>>(),
        );

        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_to_ordering_empty_map() {
        let ordering = fxhash::FxHashMap::default();

        let result = map_to_ordering(&ordering);

        let expected: MrdtSet<(char, char)> = MrdtSet::from(fxhash::FxHashSet::default());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_merge_with_complex_scenario() {
        // Create LCA
        let mut lca = MrdtOrd::default();
        lca.insert(0, 1);
        lca.insert(1, 2);
        lca.insert(2, 3);

        // Create Left
        let mut left = MrdtOrd::default();
        left.insert(0, 1);
        left.insert(1, 2);
        left.insert(2, 3);
        left.insert(3, 4);

        // Create Right
        let mut right = MrdtOrd::default();
        right.insert(0, 1);
        right.insert(1, 3);

        // Create merged_mem
        let mut merged_mem = MrdtSet::default();
        merged_mem.insert(1);
        merged_mem.insert(3);
        merged_mem.insert(4);

        // Perform merge
        let result = MrdtOrd::merge(&lca, &left, &right, &merged_mem);

        dbg!(&result);

        // Check the result
        assert_eq!(result.len(), 3);
        assert_eq!(result.index_of(&1), Some(0));
        assert_eq!(result.index_of(&3), Some(1));
        assert_eq!(result.index_of(&4), Some(2));
        assert_eq!(result.index_of(&2), None);
    }
}
