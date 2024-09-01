use super::*;

impl<T: MrdtItem + Ord> Mergeable for Vec<T> {
    fn merge(lca: &Self, left: &Self, right: &Self) -> Self {
        let lca_mem = mem(lca);
        let left_mem = mem(left);
        let right_mem = mem(right);
        let merged_mem = merge_mem(&lca_mem, &left_mem, &right_mem);

        let merged_ob = merge_ob(&ob(lca), &ob(left), &ob(right), &merged_mem);

        //If we only have one element, the ob set will be empty
        if merged_ob.is_empty() && merged_mem.len() == 1 {
            let item = merged_mem.into_iter().next().unwrap().clone();
            return vec![item];
        }

        let max_idx = merged_ob.iter().map(|(_, idx)| *idx).max().unwrap_or(0);
        let mut items: Vec<Option<T>> = vec![None; max_idx + 1];
        for (value, idx) in merged_ob {
            items[idx] = Some(value.clone());
        }
        items.into_iter().filter_map(|item| item).collect()
    }
}

fn mem<'a, T: MrdtItem>(items: &'a [T]) -> HashSet<&'a T> {
    items.iter().collect()
}

fn ob<'a, T: MrdtItem>(items: &'a [T]) -> HashSet<(&'a T, &'a T)> {
    let mut result = HashSet::default();
    let mut iter = items.iter();
    if let Some(mut prev) = iter.next() {
        for curr in iter {
            result.insert((prev, curr));
            prev = curr;
        }
    }
    result
}

pub(crate) fn merge_mem<'a, T: MrdtItem>(
    lca: &'a HashSet<&'a T>,
    left: &'a HashSet<&'a T>,
    right: &'a HashSet<&'a T>,
) -> HashSet<&'a T> {
    let mut values = lca
        .intersection(left)
        .filter(|&&item| right.contains(item))
        .copied()
        .collect::<HashSet<_>>();

    values.extend(left.difference(lca).copied());
    values.extend(right.difference(lca).copied());

    values
}

pub(crate) fn merge_ob<'a, T: 'a + MrdtItem + Ord>(
    lca: &HashSet<(&'a T, &'a T)>,
    left: &HashSet<(&'a T, &'a T)>,
    right: &HashSet<(&'a T, &'a T)>,
    merged_mem: &HashSet<&'a T>,
) -> HashMap<&'a T, usize> {
    let union = left
        .union(&right.union(lca).cloned().collect())
        .cloned()
        .collect::<HashSet<_>>();
    let set = toposort(&union);

    let mut merged_set = HashMap::default();
    for &value in set.iter() {
        if merged_mem.contains(value) {
            merged_set.insert(value, merged_set.len());
        }
    }
    merged_set
}

fn toposort<'a, T: 'a + MrdtItem + Ord>(pairs: &HashSet<(&'a T, &'a T)>) -> Vec<&'a T> {
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashMap, HashSet};

    let mut graph: HashMap<&T, Vec<&T>> = HashMap::new();
    let mut in_degree: HashMap<&T, usize> = HashMap::new();
    let mut all_nodes: HashSet<&T> = HashSet::new();

    // Build the graph and calculate in-degrees
    for &(from, to) in pairs {
        graph.entry(from).or_default().push(to);
        *in_degree.entry(to).or_insert(0) += 1;
        all_nodes.insert(from);
        all_nodes.insert(to);
    }

    // Use a BinaryHeap as a priority queue
    // Reverse is used to make it a min-heap based on (in-degree, node value)
    let mut queue: BinaryHeap<Reverse<(usize, &'a T)>> = all_nodes
        .into_iter()
        .map(|node| Reverse((*in_degree.get(node).unwrap_or(&0), node)))
        .collect();

    let mut result = Vec::new();
    let mut processed = HashSet::new();

    while let Some(Reverse((_, node))) = queue.pop() {
        if processed.contains(node) {
            continue;
        }

        result.push(node);
        processed.insert(node);

        if let Some(neighbors) = graph.get(node) {
            for &neighbor in neighbors {
                if processed.contains(neighbor) {
                    continue;
                }

                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    // Update the priority queue with the new in-degree
                    queue.push(Reverse((*degree, neighbor)));
                }
            }
        }
    }

    result
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

    impl TestItem {
        fn new(id: usize, value: String) -> Self {
            Self { id, value }
        }
    }

    #[test]
    fn test_list_merge_add() {
        let item1 = TestItem::new(1, "Item 1".into());
        let item2 = TestItem::new(2, "Item 2".into());
        let item3 = TestItem::new(3, "Item 3".into());
        let item4 = TestItem::new(4, "Item 4".into());
        let item5 = TestItem::new(5, "Item 5".into());

        let lca = vec![item1.clone(), item2.clone(), item3.clone()];

        let mut replica1 = lca.clone();
        let mut replica2 = lca.clone();

        replica1.push(item4.clone());
        replica2.remove(0);
        replica2.push(item5.clone());

        let merged_list = Mergeable::merge(&lca, &replica1, &replica2);
        assert_eq!(merged_list.len(), 4);
        assert_eq!(merged_list.iter().position(|x| x == &item2), Some(0));
        assert_eq!(merged_list.iter().position(|x| x == &item3), Some(1));
        assert_eq!(merged_list.iter().position(|x| x == &item4), Some(2));
        assert_eq!(merged_list.iter().position(|x| x == &item5), Some(3));
    }

    #[test]
    fn test_list_merge_insert() {
        let item1 = TestItem::new(1, "Item 1".into());
        let item2 = TestItem::new(2, "Item 2".into());
        let item3 = TestItem::new(3, "Item 3".into());
        let item4 = TestItem::new(4, "Item 4".into());
        let item5 = TestItem::new(5, "Item 5".into());

        let lca = vec![item1.clone(), item2.clone(), item3.clone()];

        let mut replica1 = lca.clone();
        let mut replica2 = lca.clone();

        replica1.insert(0, item4.clone());
        replica2.remove(0);
        replica2.insert(0, item5.clone());

        let merged_list = Mergeable::merge(&lca, &replica1, &replica2);
        assert_eq!(merged_list.len(), 4);
        assert_eq!(merged_list.iter().position(|x| x == &item4), Some(0));
        assert_eq!(merged_list.iter().position(|x| x == &item5), Some(1));
        assert_eq!(merged_list.iter().position(|x| x == &item2), Some(2));
        assert_eq!(merged_list.iter().position(|x| x == &item3), Some(3));
    }
}
