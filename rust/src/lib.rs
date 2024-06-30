pub mod list;
pub mod ord;
pub mod queue;
pub mod replica;
pub mod set;
pub mod storage;
pub mod vector_clock;

pub use anyhow::{Context, Result};
pub use replica::*;
pub use set::*;
pub use storage::*;
pub use vector_clock::*;

use serde::{Deserialize, Serialize};

pub trait MrdtItem: PartialEq + Eq + std::hash::Hash + Clone {}
impl<T: PartialEq + Eq + std::hash::Hash + Clone> MrdtItem for T {}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id([u8; 16]);

impl Id {
    pub const fn zero() -> Self {
        Id([0; 16])
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_slice()).unwrap()
    }
}

impl TryFrom<String> for Id {
    type Error = anyhow::Error;

    fn try_from(value: String) -> std::prelude::v1::Result<Self, Self::Error> {
        let data: [u8; 16] = value.as_bytes().try_into().unwrap();
        Ok(Id(data))
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(std::str::from_utf8(self.0.as_slice()).unwrap())
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(std::str::from_utf8(self.0.as_slice()).unwrap())
    }
}

impl Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = unsafe { std::str::from_utf8_unchecked(&self.0) };
        serializer.serialize_str(bytes)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> std::prelude::v1::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(IdDeserializer)
    }
}

struct IdDeserializer;

impl serde::de::Visitor<'_> for IdDeserializer {
    type Value = Id;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, v: &str) -> std::prelude::v1::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let data: [u8; 16] = v
            .as_bytes()
            .try_into()
            .map_err(|_| serde::de::Error::missing_field("field"))?;
        Ok(Id(data))
    }
}

impl Id {
    pub fn gen() -> Self {
        use rand::Rng;

        let data: Vec<_> = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(16)
            .collect();

        Id(data.try_into().expect("Generated invalid ID"))
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Timestamp(u32);

impl Timestamp {
    pub const fn zero() -> Self {
        Timestamp(0)
    }

    pub fn inc(self) -> Self {
        Timestamp(self.0 + 1)
    }
}

impl std::iter::Sum for Timestamp {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let sum = iter.map(|t| t.0).sum();
        Timestamp(sum)
    }
}

impl From<u32> for Timestamp {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for u32 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

#[allow(async_fn_in_trait)]
pub trait Mergeable<T> {
    fn merge(lca: &T, left: &T, right: &T) -> T;
}

pub trait Entity: serde::Serialize + serde::de::DeserializeOwned {
    fn table_name() -> &'static str;
}
