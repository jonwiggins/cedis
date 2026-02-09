pub mod entry;

use crate::glob::glob_match;
use crate::types::RedisValue;
use entry::{Entry, now_millis};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A single Redis database (one of the 16 default databases).
#[derive(Debug)]
pub struct Database {
    data: HashMap<String, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }

    /// Get a value, performing lazy expiration.
    pub fn get(&mut self, key: &str) -> Option<&Entry> {
        // Lazy expiration
        if self.is_expired(key) {
            self.data.remove(key);
            return None;
        }
        self.data.get(key)
    }

    /// Get a mutable value, performing lazy expiration.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Entry> {
        if self.is_expired(key) {
            self.data.remove(key);
            return None;
        }
        self.data.get_mut(key)
    }

    /// Set a key-value pair.
    pub fn set(&mut self, key: String, entry: Entry) {
        self.data.insert(key, entry);
    }

    /// Delete a key. Returns true if it existed.
    pub fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    /// Check if a key exists (with lazy expiration).
    pub fn exists(&mut self, key: &str) -> bool {
        if self.is_expired(key) {
            self.data.remove(key);
            return false;
        }
        self.data.contains_key(key)
    }

    /// Get the type of a key.
    pub fn key_type(&mut self, key: &str) -> Option<&'static str> {
        self.get(key).map(|e| e.value.type_name())
    }

    /// Rename a key.
    pub fn rename(&mut self, old: &str, new: &str) -> bool {
        if let Some(entry) = self.data.remove(old) {
            self.data.insert(new.to_string(), entry);
            true
        } else {
            false
        }
    }

    /// Get all keys matching a pattern.
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let now = now_millis();
        self.data
            .iter()
            .filter(|(_, entry)| {
                !entry.expires_at.is_some_and(|exp| now >= exp)
            })
            .filter(|(key, _)| glob_match(pattern, key))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Scan with cursor-based iteration.
    /// Returns (next_cursor, keys).
    pub fn scan(&self, cursor: usize, pattern: Option<&str>, count: usize) -> (usize, Vec<String>) {
        self.scan_with_type(cursor, pattern, count, None)
    }

    /// Scan with cursor-based iteration and optional type filter.
    pub fn scan_with_type(&self, cursor: usize, pattern: Option<&str>, count: usize, type_filter: Option<&str>) -> (usize, Vec<String>) {
        let now = now_millis();
        let all_keys: Vec<&String> = self
            .data
            .iter()
            .filter(|(_, entry)| {
                !entry.expires_at.is_some_and(|exp| now >= exp)
            })
            .map(|(key, _)| key)
            .collect();

        let total = all_keys.len();
        if total == 0 || cursor >= total {
            return (0, vec![]);
        }

        let mut results = Vec::new();
        let mut i = cursor;
        let mut scanned = 0;

        while i < total && scanned < count {
            let key = all_keys[i];
            let matches_pattern = pattern.map_or(true, |p| glob_match(p, key));
            let matches_type = type_filter.map_or(true, |t| {
                self.data.get(key).map_or(false, |entry| entry.value.type_name().eq_ignore_ascii_case(t))
            });
            if matches_pattern && matches_type {
                results.push(key.clone());
            }
            i += 1;
            scanned += 1;
        }

        let next_cursor = if i >= total { 0 } else { i };
        (next_cursor, results)
    }

    /// Set expiry on a key. Returns true if the key exists.
    pub fn set_expiry(&mut self, key: &str, expires_at: u64) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            entry.expires_at = Some(expires_at);
            true
        } else {
            false
        }
    }

    /// Remove expiry from a key. Returns true if the key had an expiry.
    pub fn persist(&mut self, key: &str) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }
        false
    }

    /// Number of keys (excluding expired).
    pub fn dbsize(&self) -> usize {
        let now = now_millis();
        self.data
            .iter()
            .filter(|(_, entry)| {
                !entry.expires_at.is_some_and(|exp| now >= exp)
            })
            .count()
    }

    /// Flush all data.
    pub fn flush(&mut self) {
        self.data.clear();
    }

    /// Run active expiration: sample random keys and remove expired ones.
    /// Returns the number of keys removed.
    pub fn active_expire(&mut self, sample_size: usize) -> usize {
        let now = now_millis();
        let expired_keys: Vec<String> = self
            .data
            .iter()
            .filter(|(_, entry)| entry.expires_at.is_some_and(|exp| now >= exp))
            .take(sample_size)
            .map(|(key, _)| key.clone())
            .collect();

        let count = expired_keys.len();
        for key in expired_keys {
            self.data.remove(&key);
        }
        count
    }

    /// Get a random key.
    pub fn random_key(&self) -> Option<String> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        let now = now_millis();
        self.data
            .iter()
            .filter(|(_, entry)| {
                !entry.expires_at.is_some_and(|exp| now >= exp)
            })
            .map(|(key, _)| key.clone())
            .choose(&mut rng)
    }

    fn is_expired(&self, key: &str) -> bool {
        self.data
            .get(key)
            .is_some_and(|entry| entry.is_expired())
    }

    /// Get keys with expiry info for persistence
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Entry)> {
        self.data.iter()
    }

    /// Number of keys with expiry set
    pub fn expires_count(&self) -> usize {
        self.data
            .values()
            .filter(|e| e.expires_at.is_some())
            .count()
    }

    /// Estimate memory usage of this database in bytes.
    pub fn estimated_memory(&self) -> usize {
        let mut total = 0usize;
        for (key, entry) in &self.data {
            // Key string bytes
            total += key.len();
            // Entry overhead (struct + Option<u64>)
            total += 48;
            // Value size estimate
            total += match &entry.value {
                RedisValue::String(s) => s.len(),
                RedisValue::List(l) => {
                    let element_bytes: usize = l.iter().map(|v| v.len()).sum();
                    64 * l.len() + element_bytes
                }
                RedisValue::Hash(h) => {
                    let field_bytes: usize = h.iter().map(|(k, v)| k.len() + v.len()).sum();
                    96 * h.len() + field_bytes
                }
                RedisValue::Set(s) => {
                    let member_bytes: usize = s.iter().map(|m| m.len()).sum();
                    64 * s.len() + member_bytes
                }
                RedisValue::SortedSet(z) => {
                    let member_bytes: usize = z.iter().map(|(m, _)| m.len()).sum();
                    96 * z.len() + member_bytes
                }
                RedisValue::Stream(s) => {
                    // Rough estimate: each entry has an ID (16 bytes) + fields
                    128 * s.len()
                }
                RedisValue::HyperLogLog(_) => {
                    // HyperLogLog uses a fixed 12KB register set
                    12304
                }
                RedisValue::Geo(g) => {
                    g.estimated_memory()
                }
            };
        }
        total
    }

    /// Evict one random key. Returns true if a key was evicted.
    pub fn evict_one_random(&mut self) -> bool {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        if let Some(key) = self.data.keys().choose(&mut rng).cloned() {
            self.data.remove(&key);
            true
        } else {
            false
        }
    }

    /// Evict one random key that has an expiry set. Returns true if a key was evicted.
    pub fn evict_one_volatile_random(&mut self) -> bool {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        if let Some(key) = self
            .data
            .iter()
            .filter(|(_, e)| e.expires_at.is_some())
            .map(|(k, _)| k.clone())
            .choose(&mut rng)
        {
            self.data.remove(&key);
            true
        } else {
            false
        }
    }

    /// Evict the key with the smallest TTL. Returns true if a key was evicted.
    pub fn evict_one_volatile_ttl(&mut self) -> bool {
        let key = self
            .data
            .iter()
            .filter_map(|(k, e)| e.expires_at.map(|exp| (k.clone(), exp)))
            .min_by_key(|(_, exp)| *exp)
            .map(|(k, _)| k);
        if let Some(key) = key {
            self.data.remove(&key);
            true
        } else {
            false
        }
    }
}

/// The complete data store — holds multiple databases.
#[derive(Debug)]
pub struct DataStore {
    pub databases: Vec<Database>,
}

impl DataStore {
    pub fn new(num_databases: usize) -> Self {
        let mut databases = Vec::with_capacity(num_databases);
        for _ in 0..num_databases {
            databases.push(Database::new());
        }
        DataStore { databases }
    }

    pub fn db(&mut self, index: usize) -> &mut Database {
        &mut self.databases[index]
    }

    pub fn flush_all(&mut self) {
        for db in &mut self.databases {
            db.flush();
        }
    }

    pub fn swap_db(&mut self, a: usize, b: usize) -> bool {
        if a >= self.databases.len() || b >= self.databases.len() {
            return false;
        }
        self.databases.swap(a, b);
        true
    }

    /// Run active expiration across all databases.
    pub fn active_expire_cycle(&mut self) -> usize {
        let mut total = 0;
        for db in &mut self.databases {
            total += db.active_expire(20);
        }
        total
    }

    /// Estimate total memory usage across all databases.
    pub fn estimated_memory(&self) -> usize {
        self.databases.iter().map(|db| db.estimated_memory()).sum()
    }
}

pub type SharedStore = Arc<RwLock<DataStore>>;
