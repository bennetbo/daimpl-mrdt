// use mrdt_rs::{list::MrdtList, Id, Mergeable};
// use musli::{Decode, Encode};

// #[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
// pub struct TodoStore {
//     pub todos: MrdtList<TodoItem>,
// }

// #[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
// pub struct TodoItem {
//     pub text: MrdtList<Character>,
//     pub done: bool,
// }

// #[derive(Clone, Decode, Encode, Hash, PartialEq, Eq, Debug)]
// pub struct Character {
//     id: Id,
//     c: char,
// }

// impl Mergeable<TodoItem> for TodoItem {
//     fn merge(lca: &TodoItem, left: &TodoItem, right: &TodoItem) -> TodoItem {
//         let text = MrdtList::merge(&lca.text, &left.text, &right.text);
//         let done = left.done || right.done;
//         Self { text, done }
//     }
// }

// impl Mergeable<TodoStore> for TodoStore {
//     fn merge(lca: &TodoStore, left: &TodoStore, right: &TodoStore) -> TodoStore {
//         let todos = MrdtList::merge(&lca.todos, &left.todos, &right.todos);
//         Self { todos }
//     }
// }

fn main() {}
