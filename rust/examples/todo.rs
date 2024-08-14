use mrdt_rs::{list::MrdtList, Id, Mergeable};
use musli::{Decode, Encode};
use std::cmp::Ordering;

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
pub struct TodoStore {
    pub todos: MrdtList<TodoItem>,
}

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
pub struct TodoItem {
    pub done: bool,
    pub text: MrdtList<Character>,
}

impl PartialOrd for TodoItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        for (a, b) in self.text.iter().zip(other.text.iter()) {
            match a.partial_cmp(b) {
                Some(Ordering::Less) => return Some(Ordering::Less),
                Some(Ordering::Greater) => return Some(Ordering::Greater),
                _ => continue,
            }
        }
        self.done.partial_cmp(&other.done)
    }
}

impl Ord for TodoItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

#[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Character {
    id: Id,
    c: char,
}

impl Mergeable<TodoItem> for TodoItem {
    fn merge(lca: &TodoItem, left: &TodoItem, right: &TodoItem) -> TodoItem {
        let text = MrdtList::merge(&lca.text, &left.text, &right.text);
        let done = left.done || right.done;
        Self { text, done }
    }
}

impl Mergeable<TodoStore> for TodoStore {
    fn merge(lca: &TodoStore, left: &TodoStore, right: &TodoStore) -> TodoStore {
        let todos = MrdtList::merge(&lca.todos, &left.todos, &right.todos);
        Self { todos }
    }
}

fn main() {}
