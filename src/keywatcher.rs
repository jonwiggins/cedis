use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

/// A registry that allows clients to wait for data to appear on specific keys.
/// When a producer pushes data to a key (e.g., LPUSH, RPUSH), it calls `notify`
/// to wake up any clients blocked on that key.
#[derive(Debug)]
pub struct KeyWatcher {
    /// Map from key -> list of Notify handles (one per blocked client).
    waiters: HashMap<String, Vec<Arc<Notify>>>,
}

impl KeyWatcher {
    pub fn new() -> Self {
        KeyWatcher {
            waiters: HashMap::new(),
        }
    }

    /// Register a single Notify handle across multiple keys.
    /// When any of the keys is notified, the shared handle fires.
    pub fn register_many(&mut self, keys: &[String]) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        for key in keys {
            self.waiters
                .entry(key.clone())
                .or_default()
                .push(notify.clone());
        }
        notify
    }

    /// Notify all waiters on the given key. Returns the number of waiters notified.
    pub fn notify(&mut self, key: &str) -> usize {
        if let Some(waiters) = self.waiters.remove(key) {
            let count = waiters.len();
            for w in waiters {
                w.notify_one();
            }
            count
        } else {
            0
        }
    }

    /// Remove a specific waiter from multiple keys (e.g., on timeout or after pop).
    pub fn unregister_many(&mut self, keys: &[String], notify: &Arc<Notify>) {
        for key in keys {
            if let Some(waiters) = self.waiters.get_mut(key) {
                waiters.retain(|w| !Arc::ptr_eq(w, notify));
                if waiters.is_empty() {
                    self.waiters.remove(key);
                }
            }
        }
    }
}

pub type SharedKeyWatcher = Arc<RwLock<KeyWatcher>>;
