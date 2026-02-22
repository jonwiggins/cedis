use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64};
use tokio::sync::Mutex;

/// A single slow log entry.
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub duration_micros: u64,
    pub cmd_name: String,
    pub args: Vec<String>,
}

/// The slow log — a bounded ring buffer of slow query entries.
#[derive(Debug)]
pub struct SlowLog {
    entries: VecDeque<SlowLogEntry>,
    max_len: usize,
    next_id: u64,
}

impl SlowLog {
    pub fn new(max_len: usize) -> Self {
        SlowLog {
            entries: VecDeque::with_capacity(max_len),
            max_len,
            next_id: 0,
        }
    }

    pub fn add(
        &mut self,
        timestamp: u64,
        duration_micros: u64,
        cmd_name: String,
        args: Vec<String>,
    ) {
        let entry = SlowLogEntry {
            id: self.next_id,
            timestamp,
            duration_micros,
            cmd_name,
            args,
        };
        self.next_id += 1;
        if self.entries.len() >= self.max_len {
            self.entries.pop_back();
        }
        self.entries.push_front(entry);
    }

    pub fn set_max_len(&mut self, max_len: usize) {
        self.max_len = max_len;
        while self.entries.len() > max_len {
            self.entries.pop_back();
        }
    }

    pub fn get(&self, count: usize) -> Vec<&SlowLogEntry> {
        self.entries.iter().take(count).collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.next_id = 0;
    }
}

pub type SharedSlowLog = Arc<Mutex<SlowLog>>;

/// Shared atomic for the last successful save timestamp.
pub type SharedLastSaveTime = Arc<AtomicI64>;

/// Shared atomic for tracking the server start time.
pub type SharedStartTime = Arc<AtomicU64>;

pub fn new_last_save_time() -> SharedLastSaveTime {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    Arc::new(AtomicI64::new(now))
}

pub fn new_start_time() -> SharedStartTime {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Arc::new(AtomicU64::new(now))
}
