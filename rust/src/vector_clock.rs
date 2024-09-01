use std::fmt::Display;

use super::*;
use musli::{Decode, Encode};

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct VectorClock {
    timestamps: HashMap<Id, Timestamp>,
}

impl Display for VectorClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.timestamps)
    }
}

impl VectorClock {
    pub fn merge(left: &Self, right: &Self) -> Self {
        let mut timestamps = fxhash::FxHashMap::default();

        for (id, timestamp) in left.timestamps.iter() {
            timestamps.insert(*id, *timestamp);
        }

        for (id, timestamp) in right.timestamps.iter() {
            if timestamps.contains_key(id) {
                timestamps.insert(*id, *timestamp.max(&timestamps[id]));
            } else {
                timestamps.insert(*id, *timestamp);
            }
        }

        Self { timestamps }
    }

    pub fn lca(left: &Self, right: &Self) -> Self {
        let mut timestamps = fxhash::FxHashMap::default();

        for (id, timestamp) in left.timestamps.iter() {
            if let Some(other_timestamp) = right.timestamps.get(id) {
                let timestamp = *timestamp.min(other_timestamp);
                if timestamp >= Timestamp::zero() {
                    timestamps.insert(*id, timestamp);
                }
            }
        }

        Self { timestamps }
    }

    pub fn sum(&self) -> Timestamp {
        self.timestamps.values().cloned().sum()
    }
}

impl VectorClock {
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn time_of(&self, id: Id) -> Option<Timestamp> {
        self.timestamps.get(&id).cloned()
    }

    pub fn inc(&mut self, id: Id) {
        let new_time = self.time_of(id).unwrap_or_default().inc();
        self.timestamps.insert(id, new_time);
    }
}

impl From<&[(Id, Timestamp)]> for VectorClock {
    fn from(timestamps: &[(Id, Timestamp)]) -> Self {
        let mut map = fxhash::FxHashMap::default();
        for (id, timestamp) in timestamps {
            map.insert(*id, *timestamp);
        }
        Self { timestamps: map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge() {
        let id1 = Id::gen();
        let id2 = Id::gen();
        let id3 = Id::gen();

        let vc1 =
            VectorClock::from([(id1, Timestamp::from(5)), (id2, Timestamp::from(3))].as_slice());
        let vc2 =
            VectorClock::from([(id2, Timestamp::from(4)), (id3, Timestamp::from(6))].as_slice());

        let merged = VectorClock::merge(&vc1, &vc2);
        assert_eq!(merged.time_of(id1), Some(Timestamp::from(5)));
        assert_eq!(merged.time_of(id2), Some(Timestamp::from(4)));
        assert_eq!(merged.time_of(id3), Some(Timestamp::from(6)));
    }

    #[test]
    fn test_lca() {
        let id1 = Id::gen();
        let id2 = Id::gen();

        let vc1 =
            VectorClock::from([(id1, Timestamp::from(5)), (id2, Timestamp::from(3))].as_slice());
        let vc2 = VectorClock::from([(id2, Timestamp::from(4))].as_slice());

        let lca = VectorClock::lca(&vc1, &vc2);
        assert_eq!(lca.time_of(id1), None);
        assert_eq!(lca.time_of(id2), Some(Timestamp::from(3)));
    }

    #[test]
    fn test_sum() {
        let id1 = Id::gen();
        let id2 = Id::gen();
        let id3 = Id::gen();

        let vc = VectorClock::from(
            [
                (id1, Timestamp::from(5)),
                (id2, Timestamp::from(3)),
                (id3, Timestamp::from(7)),
            ]
            .as_slice(),
        );

        assert_eq!(vc.sum(), Timestamp::from(15));
    }

    #[test]
    fn test_is_empty() {
        let vc = VectorClock::default();
        assert!(vc.is_empty());

        let id1 = Id::gen();
        let vc = VectorClock::from([(id1, Timestamp::from(1))].as_slice());
        assert!(!vc.is_empty());
    }

    #[test]
    fn test_len() {
        let vc = VectorClock::default();
        assert_eq!(vc.len(), 0);

        let id1 = Id::gen();
        let id2 = Id::gen();
        let vc =
            VectorClock::from([(id1, Timestamp::from(1)), (id2, Timestamp::from(2))].as_slice());
        assert_eq!(vc.len(), 2);
    }

    #[test]
    fn test_time_of() {
        let id1 = Id::gen();
        let id2 = Id::gen();

        let vc = VectorClock::from([(id1, Timestamp::from(2))].as_slice());

        assert_eq!(vc.time_of(id1), Some(Timestamp::from(2)));
        assert_eq!(vc.time_of(id2), None);
    }

    #[test]
    fn test_inc_new_id() {
        let mut vc = VectorClock::default();
        let id = Id::gen();

        vc.inc(id);
        assert_eq!(vc.time_of(id), Some(Timestamp::from(1)));
    }

    #[test]
    fn test_inc_multiple_times() {
        let mut vc = VectorClock::default();
        let id = Id::gen();

        for i in 1..=5 {
            vc.inc(id);
            assert_eq!(vc.time_of(id), Some(Timestamp::from(i)));
        }
    }

    #[test]
    fn test_inc_multiple_ids() {
        let mut vc = VectorClock::default();
        let id1 = Id::gen();
        let id2 = Id::gen();

        vc.inc(id1);
        vc.inc(id2);
        vc.inc(id1);

        assert_eq!(vc.time_of(id1), Some(Timestamp::from(2)));
        assert_eq!(vc.time_of(id2), Some(Timestamp::from(1)));
    }

    #[test]
    fn test_inc_from_non_zero() {
        let id = Id::gen();
        let mut vc = VectorClock::from([(id, Timestamp::from(5))].as_slice());

        vc.inc(id);
        assert_eq!(vc.time_of(id), Some(Timestamp::from(6)));
    }
}
