pub mod list;
pub mod ord;
pub mod quark;
pub mod queue;
pub mod replica;
pub mod set;
pub mod storage;
pub mod vector_clock;

pub use anyhow::{Context, Result};
use musli::{
    mode::{Binary, Text},
    Decode, Encode,
};
pub use replica::*;
pub use set::*;
pub use storage::*;
pub use vector_clock::*;

pub trait MrdtItem:
    PartialEq
    + Eq
    + std::hash::Hash
    + Clone
    + Encode<Binary>
    + Encode<Text>
    + for<'de> Decode<'de, Binary>
    + for<'de> Decode<'de, Text>
{
}
impl<
        T: PartialEq
            + Eq
            + std::hash::Hash
            + Clone
            + Encode<Binary>
            + Encode<Text>
            + for<'de> Decode<'de, Binary>
            + for<'de> Decode<'de, Text>,
    > MrdtItem for T
{
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord, Encode, Decode)]
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_ids_are_unique() {
        let entries = (0..1000)
            .into_iter()
            .map(|_| Id::gen())
            .collect::<HashSet<_>>();

        assert!(entries.len() == 1000);
    }

    #[test]
    fn test_id_generation_and_conversion() {
        let id = Id::gen();
        let str_id = id.to_string();
        assert_eq!(str_id.len(), 16);

        let converted_id = Id::try_from(str_id).unwrap();
        assert_eq!(id, converted_id);
    }

    #[test]
    fn test_id_zero() {
        let zero_id = Id::zero();
        assert_eq!(zero_id.as_str(), "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
    }
}
