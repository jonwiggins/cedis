use crate::persistence::rdb;
use crate::replication::{ReplicaInfo, ReplicaState, SharedReplicationState};
use crate::resp::{RespParser, RespValue};
use crate::store::SharedStore;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Handle the PSYNC handshake and full/partial sync with a replica.
/// This takes over the TCP connection after a PSYNC command is received.
pub async fn handle_psync(
    mut stream: TcpStream,
    replid: String,
    offset: i64,
    store: &SharedStore,
    repl_state: &SharedReplicationState,
    repl_backlog_size: usize,
) {
    let (replid_match, master_replid, master_offset) = {
        let state = repl_state.read().await;
        let matches = replid == state.master_replid || replid == "?";
        (
            matches,
            state.master_replid.clone(),
            state.master_repl_offset,
        )
    };

    // Try partial resync
    if replid != "?" && replid_match && offset > 0 {
        let partial_data = {
            let state = repl_state.read().await;
            state.backlog.as_ref().and_then(|bl| bl.read_from(offset))
        };

        if let Some(data) = partial_data {
            // Partial resync possible
            let continue_resp = format!("+CONTINUE {master_replid}\r\n");
            if stream.write_all(continue_resp.as_bytes()).await.is_err() {
                return;
            }

            // Send missing data
            if !data.is_empty() && stream.write_all(&data).await.is_err() {
                return;
            }

            info!("Partial resync with replica at offset {offset}");
            register_and_stream(stream, repl_state, master_offset).await;
            return;
        }
    }

    // Full resync
    let fullresync_resp = format!("+FULLRESYNC {master_replid} {master_offset}\r\n");
    if stream.write_all(fullresync_resp.as_bytes()).await.is_err() {
        return;
    }

    // Generate RDB in memory
    let rdb_data = {
        let store_guard = store.read().await;
        match rdb::save_to_bytes(&store_guard) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to generate RDB for replication: {e}");
                return;
            }
        }
    };

    // Send RDB as bulk string: $<len>\r\n<rdb_data>
    let header = format!("${}\r\n", rdb_data.len());
    if stream.write_all(header.as_bytes()).await.is_err() {
        return;
    }
    if stream.write_all(&rdb_data).await.is_err() {
        return;
    }

    info!(
        "Full resync with replica, sent {} bytes RDB",
        rdb_data.len()
    );

    // Ensure backlog exists
    {
        let mut state = repl_state.write().await;
        state.ensure_backlog(repl_backlog_size);
    }

    register_and_stream(stream, repl_state, master_offset).await;
}

/// Register this replica and enter the streaming loop.
async fn register_and_stream(
    mut stream: TcpStream,
    repl_state: &SharedReplicationState,
    initial_offset: i64,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<bytes::Bytes>();

    let replica_id = NEXT_REPLICA_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    {
        let mut state = repl_state.write().await;
        state.replicas.push(ReplicaInfo {
            id: replica_id,
            addr: peer_addr.clone(),
            port: 0,
            state: ReplicaState::Online,
            offset: initial_offset,
            lag: 0,
            tx,
        });
    }

    debug!("Replica {peer_addr} registered (id={replica_id})");

    let mut buf = BytesMut::with_capacity(256);

    loop {
        tokio::select! {
            // Forward replication data to replica
            data = rx.recv() => {
                match data {
                    Some(bytes) => {
                        if stream.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    None => break, // Channel closed
                }
            }
            // Read REPLCONF ACK from replica
            result = stream.read_buf(&mut buf) => {
                match result {
                    Ok(0) => break, // Disconnected
                    Ok(_) => {
                        // Parse any REPLCONF ACK responses
                        while let Ok(Some(value)) = RespParser::parse(&mut buf) {
                            if let RespValue::Array(Some(ref items)) = value
                                && items.len() >= 3
                            {
                                let cmd = items[0].to_string_lossy().unwrap_or_default().to_uppercase();
                                let sub = items[1].to_string_lossy().unwrap_or_default().to_uppercase();
                                if cmd == "REPLCONF" && sub == "ACK"
                                    && let Some(offset_str) = items[2].to_string_lossy()
                                    && let Ok(offset) = offset_str.parse::<i64>()
                                {
                                    let mut state = repl_state.write().await;
                                    if let Some(r) = state.replicas.iter_mut().find(|r| r.id == replica_id) {
                                        r.offset = offset;
                                        r.lag = 0;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    // Cleanup: remove replica
    {
        let mut state = repl_state.write().await;
        state.replicas.retain(|r| r.id != replica_id);
    }
    debug!("Replica {peer_addr} disconnected (id={replica_id})");
}

/// Handle REPLCONF command from a replica during handshake.
pub fn handle_replconf(args: &[RespValue]) -> RespValue {
    // Accept all REPLCONF subcommands during handshake
    if args.is_empty() {
        return RespValue::error("ERR wrong number of arguments for 'replconf' command");
    }

    let sub = args[0].to_string_lossy().unwrap_or_default().to_uppercase();

    match sub.as_str() {
        "LISTENING-PORT" | "CAPA" | "GETACK" => RespValue::ok(),
        "ACK" => {
            // ACK is handled in the streaming loop, but during normal command
            // dispatch we just return OK
            RespValue::ok()
        }
        _ => RespValue::ok(),
    }
}
