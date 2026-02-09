use crate::resp::RespValue;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

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
    pub watched_keys: Vec<String>,
    pub watch_dirty: bool,
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
        }
    }
}
