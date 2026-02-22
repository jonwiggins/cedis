use crate::types::RedisValue;
use std::time::{SystemTime, UNIX_EPOCH};

/// An entry in the data store — wraps a value with metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: RedisValue,
    /// Expiry time as milliseconds since UNIX epoch. None = no expiry.
    pub expires_at: Option<u64>,
    /// Last access time in seconds since UNIX epoch (for LRU eviction / OBJECT IDLETIME).
    pub last_access: u64,
}

impl Entry {
    pub fn new(value: RedisValue) -> Self {
        Entry {
            value,
            expires_at: None,
            last_access: now_seconds(),
        }
    }

    pub fn with_expiry(value: RedisValue, expires_at: u64) -> Self {
        Entry {
            value,
            expires_at: Some(expires_at),
            last_access: now_seconds(),
        }
    }

    /// Update the last access time to now.
    pub fn touch_access(&mut self) {
        self.last_access = now_seconds();
    }

    /// Return the idle time in seconds since last access.
    pub fn idle_seconds(&self) -> u64 {
        now_seconds().saturating_sub(self.last_access)
    }

    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => now_millis() >= exp,
            None => false,
        }
    }

    /// Time-to-live in seconds, or -1 if no expiry, or -2 if expired.
    pub fn ttl_seconds(&self) -> i64 {
        match self.expires_at {
            None => -1,
            Some(exp) => {
                let now = now_millis();
                if now >= exp {
                    -2
                } else {
                    ((exp - now + 500) / 1000) as i64
                }
            }
        }
    }

    /// Time-to-live in milliseconds, or -1 if no expiry, or -2 if expired.
    pub fn ttl_millis(&self) -> i64 {
        match self.expires_at {
            None => -1,
            Some(exp) => {
                let now = now_millis();
                if now >= exp { -2 } else { (exp - now) as i64 }
            }
        }
    }
}

/// Get current time in milliseconds since UNIX epoch.
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

/// Get current time in seconds since UNIX epoch.
pub fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs()
}
