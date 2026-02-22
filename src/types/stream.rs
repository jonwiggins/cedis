use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

/// A stream entry ID: milliseconds-sequence
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct StreamEntryId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamEntryId {
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamEntryId { ms, seq }
    }

    /// Parse from string like "1234-5" or "*"
    pub fn parse(s: &str) -> Option<Self> {
        if s == "*" {
            return None;
        }
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str.parse::<u64>().ok()?;
            let seq = seq_str.parse::<u64>().ok()?;
            Some(StreamEntryId { ms, seq })
        } else {
            // Just a timestamp, default seq to 0
            let ms = s.parse::<u64>().ok()?;
            Some(StreamEntryId { ms, seq: 0 })
        }
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

/// A stream entry: fields and values
pub type StreamEntry = Vec<(Vec<u8>, Vec<u8>)>;

/// An entry in the pending entries list (PEL).
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub consumer: String,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

/// A consumer within a consumer group.
#[derive(Debug, Clone)]
pub struct StreamConsumer {
    pub name: String,
    pub pending: BTreeMap<StreamEntryId, PendingEntry>,
    pub seen_time: u64,
}

impl StreamConsumer {
    pub fn new(name: String) -> Self {
        StreamConsumer {
            name,
            pending: BTreeMap::new(),
            seen_time: now_ms(),
        }
    }
}

/// A consumer group on a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_delivered_id: StreamEntryId,
    pub pel: BTreeMap<StreamEntryId, PendingEntry>,
    pub consumers: HashMap<String, StreamConsumer>,
    pub entries_read: u64,
}

impl ConsumerGroup {
    pub fn new(name: String, last_delivered_id: StreamEntryId) -> Self {
        ConsumerGroup {
            name,
            last_delivered_id,
            pel: BTreeMap::new(),
            consumers: HashMap::new(),
            entries_read: 0,
        }
    }

    /// Get or create a consumer by name. Returns a mutable reference.
    pub fn get_or_create_consumer(&mut self, name: &str) -> &mut StreamConsumer {
        if !self.consumers.contains_key(name) {
            self.consumers
                .insert(name.to_string(), StreamConsumer::new(name.to_string()));
        }
        self.consumers.get_mut(name).unwrap()
    }

    /// Create a consumer explicitly. Returns true if newly created.
    pub fn create_consumer(&mut self, name: &str) -> bool {
        if self.consumers.contains_key(name) {
            return false;
        }
        self.consumers
            .insert(name.to_string(), StreamConsumer::new(name.to_string()));
        true
    }

    /// Delete a consumer. Returns the number of pending entries that were deleted.
    pub fn delete_consumer(&mut self, name: &str) -> usize {
        if let Some(consumer) = self.consumers.remove(name) {
            let count = consumer.pending.len();
            // Remove from group PEL too
            for id in consumer.pending.keys() {
                self.pel.remove(id);
            }
            count
        } else {
            0
        }
    }

    /// Return a summary of pending entries: (total, min_id, max_id, per_consumer_counts).
    #[allow(clippy::type_complexity)]
    pub fn pending_summary(
        &self,
    ) -> (
        usize,
        Option<StreamEntryId>,
        Option<StreamEntryId>,
        Vec<(String, usize)>,
    ) {
        let total = self.pel.len();
        let min_id = self.pel.keys().next().cloned();
        let max_id = self.pel.keys().next_back().cloned();

        let mut counts: HashMap<String, usize> = HashMap::new();
        for pe in self.pel.values() {
            *counts.entry(pe.consumer.clone()).or_default() += 1;
        }
        let mut per_consumer: Vec<(String, usize)> = counts.into_iter().collect();
        per_consumer.sort_by(|a, b| a.0.cmp(&b.0));

        (total, min_id, max_id, per_consumer)
    }

    /// Return pending entries in a range, with optional consumer filter and min idle time.
    pub fn pending_range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        count: usize,
        consumer_filter: Option<&str>,
        min_idle: u64,
    ) -> Vec<(StreamEntryId, String, u64, u64)> {
        let now = now_ms();
        self.pel
            .range(start.clone()..=end.clone())
            .filter(|(_, pe)| {
                consumer_filter.is_none_or(|c| pe.consumer == c)
                    && (min_idle == 0 || now.saturating_sub(pe.delivery_time) >= min_idle)
            })
            .take(count)
            .map(|(id, pe)| {
                let idle = now.saturating_sub(pe.delivery_time);
                (id.clone(), pe.consumer.clone(), idle, pe.delivery_count)
            })
            .collect()
    }
}

/// Redis stream type
#[derive(Debug, Clone, Default)]
pub struct RedisStream {
    entries: BTreeMap<StreamEntryId, StreamEntry>,
    last_id: StreamEntryId,
    pub groups: HashMap<String, ConsumerGroup>,
}

impl RedisStream {
    pub fn new() -> Self {
        RedisStream {
            entries: BTreeMap::new(),
            last_id: StreamEntryId::new(0, 0),
            groups: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Add an entry to the stream.
    /// If `id` is None or "*", auto-generate an ID using the current time.
    /// Returns the ID of the newly added entry.
    pub fn add(&mut self, id: Option<&str>, fields: Vec<(Vec<u8>, Vec<u8>)>) -> StreamEntryId {
        let entry_id = match id {
            None | Some("*") => self.generate_id(),
            Some(s) => {
                if let Some(parsed) = StreamEntryId::parse(s) {
                    // Ensure the ID is greater than the last ID
                    if parsed <= self.last_id {
                        // If the ms is equal, increment seq; otherwise use parsed as-is
                        // but Redis would reject this; we'll just use the parsed ID
                        // and adjust if needed
                        if parsed.ms == self.last_id.ms {
                            StreamEntryId::new(parsed.ms, self.last_id.seq + 1)
                        } else {
                            // ms is less than last_id.ms, auto-generate
                            self.generate_id()
                        }
                    } else {
                        parsed
                    }
                } else {
                    self.generate_id()
                }
            }
        };

        self.last_id = entry_id.clone();
        self.entries.insert(entry_id.clone(), fields);
        entry_id
    }

    /// Return entries in the range [start, end] inclusive, ordered by ID.
    pub fn range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
    ) -> Vec<(&StreamEntryId, &StreamEntry)> {
        self.entries.range(start.clone()..=end.clone()).collect()
    }

    /// Return entries in the range [start, end] inclusive, in reverse order.
    /// Note: `start` should be >= `end` conceptually for XREVRANGE, but
    /// we accept (end, start) in BTreeMap order internally.
    pub fn rev_range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
    ) -> Vec<(&StreamEntryId, &StreamEntry)> {
        self.entries
            .range(end.clone()..=start.clone())
            .rev()
            .collect()
    }

    /// Trim the stream to at most `maxlen` entries, removing the oldest entries.
    /// Returns the number of entries trimmed.
    pub fn trim_maxlen(&mut self, maxlen: usize) -> usize {
        if self.entries.len() <= maxlen {
            return 0;
        }
        let to_remove = self.entries.len() - maxlen;
        let keys_to_remove: Vec<StreamEntryId> =
            self.entries.keys().take(to_remove).cloned().collect();
        for key in &keys_to_remove {
            self.entries.remove(key);
        }
        keys_to_remove.len()
    }

    /// Delete entries by ID. Returns the number of entries actually deleted.
    pub fn xdel(&mut self, ids: &[StreamEntryId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
            }
        }
        count
    }

    /// Return the first entry (lowest ID).
    pub fn first_entry(&self) -> Option<(&StreamEntryId, &StreamEntry)> {
        self.entries.iter().next()
    }

    /// Return the last entry (highest ID).
    pub fn last_entry(&self) -> Option<(&StreamEntryId, &StreamEntry)> {
        self.entries.iter().next_back()
    }

    /// Return a reference to the last entry ID.
    pub fn last_id(&self) -> &StreamEntryId {
        &self.last_id
    }

    /// Get a specific entry by ID.
    pub fn get_entry(&self, id: &StreamEntryId) -> Option<&StreamEntry> {
        self.entries.get(id)
    }

    /// Return the number of consumer groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Create a consumer group. Errors if the group already exists.
    pub fn create_group(&mut self, name: &str, id: StreamEntryId) -> Result<(), String> {
        if self.groups.contains_key(name) {
            return Err("BUSYGROUP Consumer Group name already exists".to_string());
        }
        self.groups
            .insert(name.to_string(), ConsumerGroup::new(name.to_string(), id));
        Ok(())
    }

    /// Destroy a consumer group. Returns true if it existed.
    pub fn destroy_group(&mut self, name: &str) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Set the last delivered ID for a consumer group.
    pub fn set_group_id(&mut self, group: &str, id: StreamEntryId) -> Result<(), String> {
        match self.groups.get_mut(group) {
            Some(g) => {
                g.last_delivered_id = id;
                Ok(())
            }
            None => Err("NOGROUP No such consumer group for key".to_string()),
        }
    }

    /// Get a reference to a consumer group.
    pub fn get_group(&self, name: &str) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// Get a mutable reference to a consumer group.
    pub fn get_group_mut(&mut self, name: &str) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    /// Read entries for a consumer group (XREADGROUP).
    /// If id is ">", deliver new entries after last_delivered_id.
    /// Otherwise, return entries from the consumer's PEL matching the given ID.
    /// Returns (entries_with_ids, is_new) where is_new indicates if ">" was used.
    pub fn read_group(
        &mut self,
        group_name: &str,
        consumer_name: &str,
        id: &str,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(StreamEntryId, StreamEntry)>, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}' for key"))?;

        if id == ">" {
            // Deliver new entries after last_delivered_id
            let start =
                StreamEntryId::new(group.last_delivered_id.ms, group.last_delivered_id.seq + 1);
            // Handle the 0-0 case: if last_delivered_id is 0-0, start from the beginning
            let effective_start = if group.last_delivered_id == StreamEntryId::new(0, 0) {
                StreamEntryId::new(0, 0)
            } else {
                start
            };
            let end = StreamEntryId::new(u64::MAX, u64::MAX);

            let entries: Vec<(StreamEntryId, StreamEntry)> = self
                .entries
                .range(effective_start..=end)
                .take(count.unwrap_or(usize::MAX))
                .map(|(id, entry)| (id.clone(), entry.clone()))
                .collect();

            let now = now_ms();
            // Update last_delivered_id and add to PEL
            for (entry_id, _) in &entries {
                group.last_delivered_id = entry_id.clone();
                group.entries_read += 1;

                if !noack {
                    let pe = PendingEntry {
                        consumer: consumer_name.to_string(),
                        delivery_time: now,
                        delivery_count: 1,
                    };
                    group.pel.insert(entry_id.clone(), pe.clone());
                    let consumer = group.get_or_create_consumer(consumer_name);
                    consumer.seen_time = now;
                    consumer.pending.insert(entry_id.clone(), pe);
                }
            }

            // Ensure consumer exists even if no entries delivered
            if entries.is_empty() {
                group.get_or_create_consumer(consumer_name).seen_time = now;
            }

            Ok(entries)
        } else {
            // Return entries from consumer's PEL
            let start = if id == "0" || id == "0-0" {
                StreamEntryId::new(0, 0)
            } else {
                StreamEntryId::parse(id).unwrap_or(StreamEntryId::new(0, 0))
            };
            let end = StreamEntryId::new(u64::MAX, u64::MAX);

            let now = now_ms();
            group.get_or_create_consumer(consumer_name).seen_time = now;

            let consumer = group.consumers.get(consumer_name);
            let pending_ids: Vec<StreamEntryId> = match consumer {
                Some(c) => c
                    .pending
                    .range(start..=end)
                    .take(count.unwrap_or(usize::MAX))
                    .map(|(id, _)| id.clone())
                    .collect(),
                None => vec![],
            };

            let mut result = Vec::new();
            for entry_id in pending_ids {
                if let Some(entry) = self.entries.get(&entry_id) {
                    result.push((entry_id, entry.clone()));
                } else {
                    // Entry was deleted from stream but still in PEL - return nil entry
                    result.push((entry_id, vec![]));
                }
            }

            Ok(result)
        }
    }

    /// Acknowledge entries in a consumer group (XACK).
    /// Removes entries from both the group PEL and the consumer PEL.
    /// Returns the number of entries acknowledged.
    pub fn xack(&mut self, group_name: &str, ids: &[StreamEntryId]) -> Result<usize, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}' for key"))?;

        let mut count = 0;
        for id in ids {
            if let Some(pe) = group.pel.remove(id) {
                count += 1;
                // Remove from consumer PEL too
                if let Some(consumer) = group.consumers.get_mut(&pe.consumer) {
                    consumer.pending.remove(id);
                }
            }
        }
        Ok(count)
    }

    /// Claim entries for a consumer (XCLAIM).
    /// Transfers pending entries from one consumer to another.
    #[allow(clippy::too_many_arguments)]
    pub fn xclaim(
        &mut self,
        group_name: &str,
        consumer_name: &str,
        min_idle: u64,
        ids: &[StreamEntryId],
        idle: Option<u64>,
        time: Option<u64>,
        retrycount: Option<u64>,
        force: bool,
        justid: bool,
    ) -> Result<Vec<(StreamEntryId, Option<StreamEntry>)>, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}' for key"))?;

        let now = now_ms();
        group.get_or_create_consumer(consumer_name);

        let mut result = Vec::new();

        for id in ids {
            let in_pel = group.pel.contains_key(id);

            if !in_pel && !force {
                continue;
            }

            if in_pel {
                let pe = group.pel.get(id).unwrap();
                let entry_idle = now.saturating_sub(pe.delivery_time);
                if entry_idle < min_idle {
                    continue;
                }
            }

            // Remove from old consumer's PEL if exists
            if let Some(old_pe) = group.pel.get(id) {
                let old_consumer_name = old_pe.consumer.clone();
                if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                    old_consumer.pending.remove(id);
                }
            }

            // Create new pending entry
            let delivery_time = if let Some(t) = time {
                t
            } else if let Some(i) = idle {
                now.saturating_sub(i)
            } else {
                now
            };

            let delivery_count = if let Some(rc) = retrycount {
                rc
            } else if let Some(old_pe) = group.pel.get(id) {
                old_pe.delivery_count + 1
            } else {
                1
            };

            let new_pe = PendingEntry {
                consumer: consumer_name.to_string(),
                delivery_time,
                delivery_count,
            };

            group.pel.insert(id.clone(), new_pe.clone());
            let consumer = group.consumers.get_mut(consumer_name).unwrap();
            consumer.seen_time = now;
            consumer.pending.insert(id.clone(), new_pe);

            if justid {
                result.push((id.clone(), None));
            } else {
                let entry = self.entries.get(id).cloned();
                result.push((id.clone(), entry));
            }
        }

        Ok(result)
    }

    /// Auto-claim idle pending entries (XAUTOCLAIM).
    /// Returns (next_cursor, claimed_entries, deleted_ids).
    #[allow(clippy::type_complexity)]
    pub fn xautoclaim(
        &mut self,
        group_name: &str,
        consumer_name: &str,
        min_idle: u64,
        start: &StreamEntryId,
        count: usize,
        justid: bool,
    ) -> Result<
        (
            StreamEntryId,
            Vec<(StreamEntryId, Option<StreamEntry>)>,
            Vec<StreamEntryId>,
        ),
        String,
    > {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}' for key"))?;

        let now = now_ms();
        group.get_or_create_consumer(consumer_name);

        let end = StreamEntryId::new(u64::MAX, u64::MAX);

        // Find idle entries in the PEL starting from `start`
        let candidates: Vec<(StreamEntryId, String)> = group
            .pel
            .range(start.clone()..=end)
            .filter(|(_, pe)| now.saturating_sub(pe.delivery_time) >= min_idle)
            .take(count + 1) // Take one extra to determine next cursor
            .map(|(id, pe)| (id.clone(), pe.consumer.clone()))
            .collect();

        let has_more = candidates.len() > count;
        let next_cursor = if has_more {
            candidates[count].0.clone()
        } else {
            StreamEntryId::new(0, 0)
        };

        let to_claim: Vec<(StreamEntryId, String)> = candidates.into_iter().take(count).collect();

        let mut claimed = Vec::new();
        let mut deleted = Vec::new();

        for (id, old_consumer_name) in to_claim {
            // Remove from old consumer
            if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                old_consumer.pending.remove(&id);
            }

            // Check if entry still exists in stream
            let entry_exists = self.entries.contains_key(&id);

            if !entry_exists {
                // Entry was deleted from stream - remove from PEL and report as deleted
                group.pel.remove(&id);
                deleted.push(id);
                continue;
            }

            // Transfer to new consumer
            let old_count = group.pel.get(&id).map(|pe| pe.delivery_count).unwrap_or(0);

            let new_pe = PendingEntry {
                consumer: consumer_name.to_string(),
                delivery_time: now,
                delivery_count: old_count + 1,
            };

            group.pel.insert(id.clone(), new_pe.clone());
            let consumer = group.consumers.get_mut(consumer_name).unwrap();
            consumer.seen_time = now;
            consumer.pending.insert(id.clone(), new_pe);

            if justid {
                claimed.push((id, None));
            } else {
                let entry = self.entries.get(&id).cloned();
                claimed.push((id, entry));
            }
        }

        Ok((next_cursor, claimed, deleted))
    }

    /// Generate an auto-incrementing ID based on the current time.
    fn generate_id(&self) -> StreamEntryId {
        let now_millis = now_ms();

        if now_millis > self.last_id.ms {
            StreamEntryId::new(now_millis, 0)
        } else {
            // Same or earlier millisecond: use last_id's ms and increment seq
            StreamEntryId::new(self.last_id.ms, self.last_id.seq + 1)
        }
    }
}
