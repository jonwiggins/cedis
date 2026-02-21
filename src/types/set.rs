use std::collections::HashSet;

/// Redis set type.
#[derive(Debug, Clone, Default)]
pub struct RedisSet {
    data: HashSet<Vec<u8>>,
    /// Once a non-integer member is added, the set can never go back to intset encoding.
    was_non_intset: bool,
}

impl RedisSet {
    pub fn new() -> Self {
        RedisSet {
            data: HashSet::new(),
            was_non_intset: false,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Add a member. Returns true if the member was new.
    pub fn add(&mut self, member: Vec<u8>) -> bool {
        // Track if a non-integer was ever added (prevents downgrade to intset encoding)
        if !self.was_non_intset {
            let is_int = std::str::from_utf8(&member)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_some();
            if !is_int {
                self.was_non_intset = true;
            }
        }
        self.data.insert(member)
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.data.remove(member)
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        self.data.contains(member)
    }

    pub fn members(&self) -> Vec<&Vec<u8>> {
        self.data.iter().collect()
    }

    pub fn union(&self, other: &RedisSet) -> RedisSet {
        RedisSet {
            data: self.data.union(&other.data).cloned().collect(),
            was_non_intset: self.was_non_intset || other.was_non_intset,
        }
    }

    pub fn intersect(&self, other: &RedisSet) -> RedisSet {
        RedisSet {
            data: self.data.intersection(&other.data).cloned().collect(),
            was_non_intset: self.was_non_intset || other.was_non_intset,
        }
    }

    pub fn difference(&self, other: &RedisSet) -> RedisSet {
        RedisSet {
            data: self.data.difference(&other.data).cloned().collect(),
            was_non_intset: self.was_non_intset || other.was_non_intset,
        }
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        let member = self.data.iter().next()?.clone();
        self.data.remove(&member);
        Some(member)
    }

    pub fn random_member(&self) -> Option<&Vec<u8>> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.data.iter().choose(&mut rng)
    }

    pub fn random_members(&self, count: i64) -> Vec<Vec<u8>> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        if count > 0 {
            // Unique elements
            let count = (count as usize).min(self.data.len());
            self.data
                .iter()
                .choose_multiple(&mut rng, count)
                .into_iter()
                .cloned()
                .collect()
        } else {
            // May repeat
            let count = (-count) as usize;
            let members: Vec<&Vec<u8>> = self.data.iter().collect();
            if members.is_empty() {
                return vec![];
            }
            (0..count)
                .map(|_| {
                    use rand::Rng;
                    let idx = rng.gen_range(0..members.len());
                    members[idx].clone()
                })
                .collect()
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.data.iter()
    }

    pub fn from_set(data: HashSet<Vec<u8>>) -> Self {
        let was_non_intset = data.iter().any(|m| {
            std::str::from_utf8(m)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_none()
        });
        RedisSet {
            data,
            was_non_intset,
        }
    }

    pub fn into_inner(self) -> HashSet<Vec<u8>> {
        self.data
    }

    /// Check if any member exceeds the given byte length.
    pub fn has_long_entry(&self, max_bytes: usize) -> bool {
        self.data.iter().any(|m| m.len() > max_bytes)
    }

    /// Check if all members are integers and the set was never promoted from intset.
    pub fn is_all_integers(&self) -> bool {
        !self.was_non_intset
            && self.data.iter().all(|member| {
                std::str::from_utf8(member)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .is_some()
            })
    }
}
