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

impl<T: MrdtItem + Ord> MrdtOrd<T> {
    pub fn merge(lca: &Self, left: &Self, right: &Self, merged_mem: &MrdtSet<T>) -> Self {
        let left = map_to_ordering(&left.store);
        let right = map_to_ordering(&right.store);
        let lca = map_to_ordering(&lca.store);

        let merged_ord = merge_sets(&lca, &left, &right);

        let mut merged = merged_ord.clone();
        for (k, v) in merged_ord.iter() {
            if !merged_mem.contains(k) || !merged_mem.contains(v) {
                merged.remove(&(k.clone(), v.clone()));
            }
        }

        Self {
            store: ordering_to_hashmap(&merged.store),
        }
    }
}

impl<T: MrdtItem> Default for MrdtOrd<T> {
    fn default() -> Self {
        Self {
            store: fxhash::FxHashMap::default(),
        }
    }
}

fn ordering_to_hashmap<T: Ord + Clone + std::hash::Hash>(
    ordering: &fxhash::FxHashSet<(T, T)>,
) -> fxhash::FxHashMap<T, usize> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Define auxiliary structures
    let nodes: fxhash::FxHashSet<T> = ordering
        .iter()
        .flat_map(|(a, b)| vec![a.clone(), b.clone()])
        .collect();

    let mut adjacency_list: fxhash::FxHashMap<T, Vec<T>> = fxhash::FxHashMap::default();
    let mut in_degree: fxhash::FxHashMap<T, usize> = fxhash::FxHashMap::default();

    for (from, to) in ordering {
        adjacency_list
            .entry(from.clone())
            .or_default()
            .push(to.clone());

        *in_degree.entry(to.clone()).or_insert(0) += 1;
        in_degree.entry(from.clone()).or_insert(0);
    }

    for (_k, v) in adjacency_list.iter_mut() {
        v.sort();
    }

    // Priority Queue for maintaining lexical order among available nodes
    let mut queue: BinaryHeap<Reverse<T>> = BinaryHeap::new();

    // Enqueue nodes with no in-degrees
    for node in &nodes {
        if *in_degree.get(node).unwrap() == 0 {
            queue.push(Reverse(node.clone()));
        }
    }

    let mut result = Vec::new();

    while let Some(Reverse(current)) = queue.pop() {
        result.push(current.clone());

        if let Some(neighbors) = adjacency_list.get(&current) {
            for neighbor in neighbors {
                let in_deg = in_degree.get_mut(neighbor).unwrap();
                *in_deg -= 1;

                if *in_deg == 0 {
                    queue.push(Reverse(neighbor.clone()));
                }
            }
        }
    }

    let mut map = fxhash::FxHashMap::default();
    for (idx, value) in result.into_iter().enumerate() {
        map.insert(value, idx);
    }
    map
}

fn map_to_ordering<T: MrdtItem>(ordering: &fxhash::FxHashMap<T, usize>) -> MrdtSet<(T, T)> {
    let mut ordered_set = fxhash::FxHashSet::default();
    let mut sorted_items: Vec<_> = ordering.iter().collect();
    sorted_items.sort_by_key(|(_, &idx)| idx);

    for (i, (value, _)) in sorted_items.iter().enumerate() {
        for (value2, _) in &sorted_items[i + 1..] {
            ordered_set.insert(((*value).clone(), (*value2).clone()));
        }
    }

    MrdtSet::from(ordered_set)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordering_to_hashmap() {
        let mut ordering = fxhash::FxHashSet::default();
        ordering.insert(('a', 'b'));
        ordering.insert(('b', 'c'));
        ordering.insert(('a', 'c'));

        let result = ordering_to_hashmap(&ordering);

        let expected: fxhash::FxHashMap<char, usize> =
            [('a', 0), ('b', 1), ('c', 2)].iter().cloned().collect();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_ordering_with_cycle() {
        let mut ordering = fxhash::FxHashSet::default();
        ordering.insert(('a', 'b'));
        ordering.insert(('b', 'c'));
        ordering.insert(('c', 'a'));

        let result = ordering_to_hashmap(&ordering);
        // In case of cycle detection, we return an empty map
        assert!(result.is_empty());
    }

    #[test]
    fn test_ordering_with_single_node() {
        let ordering = fxhash::FxHashSet::default();
        let result = ordering_to_hashmap(&ordering);
        let expected: fxhash::FxHashMap<char, usize> = fxhash::FxHashMap::default();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_to_ordering() {
        let mut ordering = fxhash::FxHashMap::default();
        ordering.insert('a', 0);
        ordering.insert('b', 1);
        ordering.insert('c', 2);

        let result = map_to_ordering(&ordering);

        let expected: MrdtSet<(char, char)> = MrdtSet::from(
            vec![('a', 'b'), ('a', 'c'), ('b', 'c')]
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
}
