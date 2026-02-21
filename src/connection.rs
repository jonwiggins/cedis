use crate::resp::RespValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Shared broadcast channel for MONITOR command.
pub type MonitorSender = Arc<broadcast::Sender<String>>;

pub fn new_monitor_sender() -> MonitorSender {
    Arc::new(broadcast::channel(256).0)
}

/// Per-client connection state.
#[derive(Debug)]
pub struct ClientState {
    pub id: u64,
    pub db_index: usize,
    pub authenticated: bool,
    pub should_close: bool,
    pub name: Option<String>,

    // Transaction state
    pub in_multi: bool,
    pub multi_queue: Vec<(String, Vec<RespValue>)>,
    /// (db_index, key, version_at_watch_time, global_version_at_watch_time)
    pub watched_keys: Vec<(usize, String, u64, u64)>,
    pub watch_dirty: bool,
    pub multi_error: bool,

    // Pub/Sub state — number of active subscriptions (channels + patterns)
    pub subscriptions: usize,

    // Monitor mode
    pub in_monitor: bool,
}

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            db_index: 0,
            authenticated: false,
            should_close: false,
            name: None,
            in_multi: false,
            multi_queue: Vec::new(),
            watched_keys: Vec::new(),
            watch_dirty: false,
            multi_error: false,
            subscriptions: 0,
            in_monitor: false,
        }
    }

    /// Whether this client is in subscribe mode (can only run subscribe commands).
    pub fn in_subscribe_mode(&self) -> bool {
        self.subscriptions > 0
    }
}
