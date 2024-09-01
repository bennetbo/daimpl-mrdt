use mrdt_rs::*;
use rand::Rng;
// Import other necessary items

fn main() {
    // Create test data
    let lca = create_test_list(2500);
    let mut left = lca.clone();
    let mut right = lca.clone();
    insert_random_ids(&mut left, 250);
    insert_random_ids(&mut right, 250);

    // Run the merge operation multiple times to get a good profile
    let _ = Mergeable::merge(&lca, &left, &right);
}

fn create_test_list(length: usize) -> Vec<Id> {
    let mut doc = Vec::default();
    for _ in 0..length {
        doc.push(Id::gen());
    }
    doc
}

fn insert_random_ids(list: &mut Vec<Id>, insertion_count: usize) {
    for _ in 0..insertion_count {
        list.insert(rand::thread_rng().gen_range(0..list.len()), Id::gen());
    }
}
