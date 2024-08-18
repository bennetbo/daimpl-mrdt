use std::{cell::RefCell, collections::HashMap, hash::Hasher, sync::Arc};

use anyhow::{Context, Result};
use musli::{
    de::DecodeOwned,
    mode::Binary,
    storage::{Encoding, OPTIONS},
    Encode,
};
use scylla::Session;
use std::hash::Hash;

const ENCODING: Encoding<OPTIONS> = Encoding::new().with_options();

#[derive(Clone, Debug)]
pub struct Ref {
    pub id: u64,
    pub left: Option<u64>,
    pub right: Option<u64>,
    pub object_ref: u64,
}

pub enum Store {
    Scylla(Session),
    #[cfg(test)]
    Fake {
        objects: RefCell<HashMap<u64, Vec<u8>>>,
        refs: RefCell<HashMap<u64, Ref>>,
    },
}

impl Store {
    #[cfg(test)]
    pub fn test() -> Self {
        Self::Fake {
            objects: RefCell::new(HashMap::new()),
            refs: RefCell::new(HashMap::new()),
        }
    }

    pub async fn resolve<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
        match self {
            Store::Scylla(_) => todo!(),
            #[cfg(test)]
            Store::Fake { objects, .. } => {
                let objects = objects.borrow();
                let Some(bytes) = objects.get(&id) else {
                    return Ok(None);
                };
                let decoded = ENCODING
                    .decode(bytes.as_slice())
                    .with_context(|| "Failed to deserialize object")?;

                Ok(Some(decoded))
            }
        }
    }

    pub async fn insert<T: Hash + Encode<Binary>>(&self, object: T) -> Result<u64> {
        match self {
            Store::Scylla(_) => todo!(),
            #[cfg(test)]
            Store::Fake { objects, .. } => {
                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                let hash = hasher.finish();

                let mut bytes = Vec::new();
                ENCODING
                    .encode(&mut bytes, &object)
                    .with_context(|| "Failed to serialize object")?;

                let mut objects = objects.borrow_mut();
                objects.insert(hash, bytes);
                Ok(hash)
            }
        }
    }

    #[cfg(test)]
    pub fn insert_ref(&self, reference: Ref) {
        match self {
            Store::Scylla(_) => todo!(),
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let mut refs = refs.borrow_mut();
                refs.insert(reference.id, reference);
            }
        }
    }

    pub async fn resolve_ref(&self, id: Option<u64>) -> Result<Option<Ref>> {
        match self {
            Store::Scylla(_) => todo!(),
            #[cfg(test)]
            Store::Fake { refs, .. } => {
                let refs = refs.borrow();
                dbg!(&refs, &id);
                let Some(id) = id else {
                    return Ok(None);
                };
                Ok(refs.get(&id).cloned())
            }
        }
    }
}

struct SerializeCx {
    store: Arc<Store>,
}

impl SerializeCx {
    pub async fn object_ref<T: Hash + Encode<Binary>>(&self, object: T) -> Result<u64> {
        self.store.insert(object).await
    }
}

struct DeserializeCx {
    store: Arc<Store>,
}

impl DeserializeCx {
    pub async fn resolve<T: Hash + DecodeOwned<Binary>>(&self, id: u64) -> Result<Option<T>> {
        self.store.resolve(id).await
    }

    pub async fn resolve_ref(&self, id: Option<u64>) -> Result<Option<Ref>> {
        self.store.resolve_ref(id).await
    }
}

trait Serialize {
    async fn serialize(&self, cx: &SerializeCx) -> Result<Vec<Ref>>;
}

trait Deserialize: Sized {
    async fn deserialize(root: Ref, cx: &DeserializeCx) -> Result<Self>;
}

#[cfg(test)]
mod tests {
    use std::hash::{Hash, Hasher};

    use super::*;

    #[derive(Debug)]
    struct List {
        items: Vec<String>,
    }

    impl Serialize for List {
        async fn serialize(&self, cx: &SerializeCx) -> Result<Vec<Ref>> {
            let mut refs = Vec::with_capacity(self.items.len());
            let mut last_hash: Option<u64> = None;
            for object in self.items.iter() {
                let mut hasher = std::hash::DefaultHasher::new();
                object.hash(&mut hasher);
                if let Some(last_hash) = last_hash {
                    last_hash.hash(&mut hasher);
                }
                let hash = hasher.finish();

                let object_ref = cx.object_ref(object).await?;
                refs.push(Ref {
                    id: hash,
                    left: last_hash,
                    right: None,
                    object_ref,
                });
                last_hash = Some(hash);
            }
            Ok(refs)
        }
    }

    impl Deserialize for List {
        async fn deserialize(root: Ref, cx: &DeserializeCx) -> Result<Self> {
            let mut items = Vec::new();
            items.push(
                cx.resolve(root.object_ref)
                    .await?
                    .with_context(|| "Missing object")?,
            );

            let mut node = root;
            while let Some(new_node) = cx.resolve_ref(node.left).await? {
                items.push(
                    cx.resolve(new_node.object_ref)
                        .await?
                        .with_context(|| "Missing object")?,
                );
                node = new_node
            }
            items.reverse();
            Ok(Self { items })
        }
    }

    #[tokio::test]
    async fn test_serialize_deserialize() {
        let list = List {
            items: vec!["1".to_string(), "2".to_string(), "3".to_string()],
        };

        let store = Arc::new(Store::test());

        let cx = SerializeCx {
            store: store.clone(),
        };

        let refs = list.serialize(&cx).await.unwrap();

        for reference in refs.iter() {
            store.insert_ref(reference.clone());
        }

        let cx = DeserializeCx {
            store: store.clone(),
        };

        let root = refs.last().cloned().unwrap();
        let deserialized = List::deserialize(root, &cx).await.unwrap();

        dbg!(deserialized);
    }
}
