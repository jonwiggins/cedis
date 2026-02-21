use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// A stream entry ID: milliseconds-sequence
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

/// A stream entry: fields and values
pub type StreamEntry = Vec<(Vec<u8>, Vec<u8>)>;

/// Redis stream type
#[derive(Debug, Clone)]
pub struct RedisStream {
    entries: BTreeMap<StreamEntryId, StreamEntry>,
    last_id: StreamEntryId,
}

impl RedisStream {
    pub fn new() -> Self {
        RedisStream {
            entries: BTreeMap::new(),
            last_id: StreamEntryId::new(0, 0),
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
        self.entries
            .range(start.clone()..=end.clone())
            .collect()
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
        let keys_to_remove: Vec<StreamEntryId> = self
            .entries
            .keys()
            .take(to_remove)
            .cloned()
            .collect();
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

    /// Generate an auto-incrementing ID based on the current time.
    fn generate_id(&self) -> StreamEntryId {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64;

        if now_ms > self.last_id.ms {
            StreamEntryId::new(now_ms, 0)
        } else {
            // Same or earlier millisecond: use last_id's ms and increment seq
            StreamEntryId::new(self.last_id.ms, self.last_id.seq + 1)
        }
    }
}
