pub mod backlog;
pub mod master;
pub mod replica;

use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Master,
    Replica,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaState {
    Handshake,
    WaitBgsave,
    SendBulk,
    Online,
}

impl std::fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaState::Handshake => write!(f, "handshake"),
            ReplicaState::WaitBgsave => write!(f, "wait_bgsave"),
            ReplicaState::SendBulk => write!(f, "send_bulk"),
            ReplicaState::Online => write!(f, "online"),
        }
    }
}

/// Info about a connected replica (from master's perspective).
#[derive(Debug)]
pub struct ReplicaInfo {
    pub id: u64,
    pub addr: String,
    pub port: u16,
    pub state: ReplicaState,
    pub offset: i64,
    pub lag: u64,
    pub tx: mpsc::UnboundedSender<bytes::Bytes>,
}

/// The replication state shared across the server.
#[derive(Debug)]
pub struct ReplicationState {
    pub role: ReplicationRole,
    pub master_replid: String,
    pub master_replid2: String,
    pub master_repl_offset: i64,
    pub second_repl_offset: i64,
    pub replicas: Vec<ReplicaInfo>,
    pub backlog: Option<backlog::ReplicationBacklog>,
    // Replica-specific
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub master_link_status: String,
    pub master_sync_in_progress: bool,
    pub cancel: Option<tokio_util::sync::CancellationToken>,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationState {
    pub fn new() -> Self {
        ReplicationState {
            role: ReplicationRole::Master,
            master_replid: generate_replid(),
            master_replid2: "0000000000000000000000000000000000000000".to_string(),
            master_repl_offset: 0,
            second_repl_offset: -1,
            replicas: Vec::new(),
            backlog: None,
            master_host: None,
            master_port: None,
            master_link_status: "up".to_string(),
            master_sync_in_progress: false,
            cancel: None,
        }
    }

    /// Ensure the replication backlog exists with the given size.
    pub fn ensure_backlog(&mut self, size: usize) {
        if self.backlog.is_none() {
            self.backlog = Some(backlog::ReplicationBacklog::new(size));
        }
    }

    /// Append data to the replication backlog and increment offset.
    pub fn feed_backlog(&mut self, data: &[u8]) {
        self.master_repl_offset += data.len() as i64;
        if let Some(ref mut bl) = self.backlog {
            bl.append(data, self.master_repl_offset);
        }
    }

    /// Send replication data to all connected replicas.
    /// Removes replicas where the send channel is closed.
    pub fn propagate_to_replicas(&mut self, data: &[u8]) {
        let data = bytes::Bytes::copy_from_slice(data);
        self.replicas
            .retain(|r| r.state == ReplicaState::Online && r.tx.send(data.clone()).is_ok());
    }

    /// Count of connected replicas in Online state.
    pub fn connected_slaves(&self) -> usize {
        self.replicas
            .iter()
            .filter(|r| r.state == ReplicaState::Online)
            .count()
    }
}

/// Generate a 40-character hex replication ID.
fn generate_replid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..20).map(|_| rng.r#gen()).collect();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub type SharedReplicationState = Arc<RwLock<ReplicationState>>;
