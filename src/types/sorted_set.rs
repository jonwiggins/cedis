use std::collections::{BTreeMap, HashMap};

/// Redis sorted set — implemented using BTreeMap + HashMap.
/// The BTreeMap provides ordered iteration by score,
/// and the HashMap provides O(1) score lookup by member.
#[derive(Debug, Clone)]
pub struct RedisSortedSet {
    /// member -> score
    scores: HashMap<Vec<u8>, f64>,
    /// (score, member) -> () — for ordered iteration
    /// We use (OrderedFloat, member) as the key to get proper ordering.
    tree: BTreeMap<SortedSetKey, ()>,
}

/// Key for the BTreeMap — sorts by score first, then by member lexicographically.
#[derive(Debug, Clone, Eq, PartialEq)]
struct SortedSetKey {
    score_bits: u64, // IEEE 754 bits, transformed for ordering
    member: Vec<u8>,
}

impl SortedSetKey {
    fn new(score: f64, member: Vec<u8>) -> Self {
        SortedSetKey {
            score_bits: f64_to_orderable(score),
            member,
        }
    }
}

impl Ord for SortedSetKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score_bits
            .cmp(&other.score_bits)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for SortedSetKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Transform f64 bits so that standard u64 ordering matches f64 ordering.
fn f64_to_orderable(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits >> 63 == 1 {
        // Negative: flip all bits
        !bits
    } else {
        // Positive: flip sign bit
        bits ^ (1 << 63)
    }
}

impl RedisSortedSet {
    pub fn new() -> Self {
        RedisSortedSet {
            scores: HashMap::new(),
            tree: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Add or update a member. Returns true if the member was new.
    pub fn add(&mut self, member: Vec<u8>, score: f64) -> bool {
        if let Some(&old_score) = self.scores.get(&member) {
            // Remove old entry from tree
            self.tree
                .remove(&SortedSetKey::new(old_score, member.clone()));
            self.scores.insert(member.clone(), score);
            self.tree.insert(SortedSetKey::new(score, member), ());
            false
        } else {
            self.scores.insert(member.clone(), score);
            self.tree.insert(SortedSetKey::new(score, member), ());
            true
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.tree
                .remove(&SortedSetKey::new(score, member.to_vec()));
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// Get the rank (0-based) of a member in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let key = SortedSetKey::new(*score, member.to_vec());
        Some(self.tree.range(..&key).count())
    }

    /// Get the rank (0-based) of a member in descending order.
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Get members in ascending order by rank range.
    pub fn range(&self, start: i64, stop: i64) -> Vec<(&[u8], f64)> {
        let len = self.len() as i64;
        let start = normalize_index(start, len);
        let stop = normalize_index(stop, len);

        if start > stop || start >= self.len() {
            return vec![];
        }

        let stop = stop.min(self.len() - 1);

        self.tree
            .keys()
            .skip(start)
            .take(stop - start + 1)
            .map(|k| (k.member.as_slice(), *self.scores.get(&k.member).unwrap()))
            .collect()
    }

    /// Get members in descending order by rank range.
    pub fn rev_range(&self, start: i64, stop: i64) -> Vec<(&[u8], f64)> {
        let len = self.len() as i64;
        let start = normalize_index(start, len);
        let stop = normalize_index(stop, len);

        if start > stop || start >= self.len() {
            return vec![];
        }

        let stop = stop.min(self.len() - 1);

        self.tree
            .keys()
            .rev()
            .skip(start)
            .take(stop - start + 1)
            .map(|k| (k.member.as_slice(), *self.scores.get(&k.member).unwrap()))
            .collect()
    }

    /// Get members with scores in [min, max].
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(&[u8], f64)> {
        let min_key = SortedSetKey::new(min, vec![]);
        let max_key = SortedSetKey::new(max, vec![0xff; 128]);

        self.tree
            .range(min_key..=max_key)
            .filter(|(k, _)| {
                let score = *self.scores.get(&k.member).unwrap();
                score >= min && score <= max
            })
            .map(|(k, _)| (k.member.as_slice(), *self.scores.get(&k.member).unwrap()))
            .collect()
    }

    /// Count members with scores in [min, max].
    pub fn count(&self, min: f64, max: f64) -> usize {
        self.range_by_score(min, max).len()
    }

    /// Increment a member's score. Returns the new score.
    pub fn incr_by(&mut self, member: Vec<u8>, delta: f64) -> f64 {
        let current = self.scores.get(&member).copied().unwrap_or(0.0);
        let new_score = current + delta;
        self.add(member, new_score);
        new_score
    }

    /// Pop the member with the minimum score.
    pub fn pop_min(&mut self) -> Option<(Vec<u8>, f64)> {
        let key = self.tree.keys().next()?.clone();
        let score = *self.scores.get(&key.member)?;
        self.remove(&key.member.clone());
        Some((key.member, score))
    }

    /// Pop the member with the maximum score.
    pub fn pop_max(&mut self) -> Option<(Vec<u8>, f64)> {
        let key = self.tree.keys().next_back()?.clone();
        let score = *self.scores.get(&key.member)?;
        self.remove(&key.member.clone());
        Some((key.member, score))
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        self.scores.contains_key(member)
    }

    /// Check if any member exceeds the given byte length.
    pub fn has_long_entry(&self, max_bytes: usize) -> bool {
        self.scores.keys().any(|m| m.len() > max_bytes)
    }

    /// Iterator over all (member, score) pairs in score order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], f64)> {
        self.tree
            .keys()
            .map(|k| (k.member.as_slice(), *self.scores.get(&k.member).unwrap()))
    }

    /// Get range by lex (for members with equal scores).
    pub fn range_by_lex(
        &self,
        min: &[u8],
        min_inclusive: bool,
        max: &[u8],
        max_inclusive: bool,
    ) -> Vec<(&[u8], f64)> {
        self.tree
            .keys()
            .filter(|k| {
                let m = k.member.as_slice();
                let above_min = if min.is_empty() {
                    true
                } else if min_inclusive {
                    m >= min
                } else {
                    m > min
                };
                let below_max = if max.is_empty() {
                    true
                } else if max_inclusive {
                    m <= max
                } else {
                    m < max
                };
                above_min && below_max
            })
            .map(|k| (k.member.as_slice(), *self.scores.get(&k.member).unwrap()))
            .collect()
    }
}

fn normalize_index(index: i64, len: i64) -> usize {
    if index < 0 {
        (len + index).max(0) as usize
    } else {
        index as usize
    }
}
