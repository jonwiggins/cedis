use crate::command;
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::persistence::rdb;
use crate::pubsub::SharedPubSub;
use crate::replication::SharedReplicationState;
use crate::resp::{RespParser, RespValue};
use crate::scripting::ScriptCache;
use crate::store::SharedStore;
use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Run the replica synchronization loop.
/// Connects to the master, performs handshake, receives RDB, then processes
/// the command stream. Retries on disconnect with exponential backoff.
#[allow(clippy::too_many_arguments)]
pub async fn replica_sync_loop(
    host: String,
    port: u16,
    store: SharedStore,
    config: SharedConfig,
    repl_state: SharedReplicationState,
    pubsub: SharedPubSub,
    key_watcher: SharedKeyWatcher,
    script_cache: ScriptCache,
    cancel: CancellationToken,
) {
    let mut retry_delay = Duration::from_secs(1);
    let max_retry_delay = Duration::from_secs(30);

    loop {
        if cancel.is_cancelled() {
            info!("Replica sync cancelled");
            return;
        }

        {
            let mut state = repl_state.write().await;
            state.master_link_status = "down".to_string();
            state.master_sync_in_progress = false;
        }

        let addr = format!("{host}:{port}");
        info!("Connecting to master at {addr}...");

        let connect_result = tokio::select! {
            result = TcpStream::connect(&addr) => result,
            _ = cancel.cancelled() => {
                info!("Replica sync cancelled during connect");
                return;
            }
        };

        match connect_result {
            Ok(stream) => {
                retry_delay = Duration::from_secs(1); // Reset on successful connect
                match run_sync(
                    stream,
                    &store,
                    &config,
                    &repl_state,
                    &pubsub,
                    &key_watcher,
                    &script_cache,
                    &cancel,
                )
                .await
                {
                    Ok(()) => {
                        info!("Replica sync ended cleanly");
                    }
                    Err(e) => {
                        warn!("Replica sync error: {e}");
                    }
                }
            }
            Err(e) => {
                warn!("Failed to connect to master at {addr}: {e}");
            }
        }

        if cancel.is_cancelled() {
            return;
        }

        info!("Retrying in {}s...", retry_delay.as_secs());
        tokio::select! {
            _ = tokio::time::sleep(retry_delay) => {},
            _ = cancel.cancelled() => return,
        }
        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sync(
    mut stream: TcpStream,
    store: &SharedStore,
    config: &SharedConfig,
    repl_state: &SharedReplicationState,
    pubsub: &SharedPubSub,
    key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let mut buf = BytesMut::with_capacity(8192);

    // Step 1: Send PING
    send_command(&mut stream, &["PING"])
        .await
        .map_err(|e| format!("PING failed: {e}"))?;
    let resp = read_response(&mut stream, &mut buf, cancel)
        .await
        .map_err(|e| format!("PING response failed: {e}"))?;
    debug!("Master PING response: {resp:?}");

    // Step 2: REPLCONF listening-port
    let port = {
        let cfg = config.read().await;
        cfg.port.to_string()
    };
    send_command(&mut stream, &["REPLCONF", "listening-port", &port])
        .await
        .map_err(|e| format!("REPLCONF listening-port failed: {e}"))?;
    let _resp = read_response(&mut stream, &mut buf, cancel)
        .await
        .map_err(|e| format!("REPLCONF listening-port response failed: {e}"))?;

    // Step 3: REPLCONF capa
    send_command(&mut stream, &["REPLCONF", "capa", "psync2"])
        .await
        .map_err(|e| format!("REPLCONF capa failed: {e}"))?;
    let _resp = read_response(&mut stream, &mut buf, cancel)
        .await
        .map_err(|e| format!("REPLCONF capa response failed: {e}"))?;

    // Step 4: PSYNC
    let (replid, offset) = {
        let state = repl_state.read().await;
        (state.master_replid.clone(), state.master_repl_offset)
    };
    // First sync: PSYNC ? -1
    let psync_replid = if offset == 0 { "?" } else { &replid };
    let psync_offset = if offset == 0 {
        "-1".to_string()
    } else {
        offset.to_string()
    };
    send_command(&mut stream, &["PSYNC", psync_replid, &psync_offset])
        .await
        .map_err(|e| format!("PSYNC failed: {e}"))?;

    // Read PSYNC response: +FULLRESYNC <replid> <offset> or +CONTINUE <replid>
    let psync_resp = read_line(&mut stream, &mut buf, cancel)
        .await
        .map_err(|e| format!("PSYNC response failed: {e}"))?;

    if psync_resp.starts_with("+FULLRESYNC") {
        let parts: Vec<&str> = psync_resp.splitn(3, ' ').collect();
        if parts.len() >= 3 {
            let new_replid = parts[1].to_string();
            let new_offset: i64 = parts[2].trim().parse().unwrap_or(0);

            {
                let mut state = repl_state.write().await;
                state.master_replid = new_replid;
                state.master_repl_offset = new_offset;
                state.master_sync_in_progress = true;
            }
        }

        // Receive RDB bulk transfer
        info!("Full resync - receiving RDB...");
        receive_rdb(&mut stream, &mut buf, store, config, cancel).await?;

        {
            let mut state = repl_state.write().await;
            state.master_link_status = "up".to_string();
            state.master_sync_in_progress = false;
        }
        info!("Full resync complete, entering streaming mode");
    } else if psync_resp.starts_with("+CONTINUE") {
        let parts: Vec<&str> = psync_resp.splitn(2, ' ').collect();
        if parts.len() >= 2 {
            let new_replid = parts[1].trim().to_string();
            let mut state = repl_state.write().await;
            state.master_replid = new_replid;
            state.master_link_status = "up".to_string();
        }
        info!("Partial resync, entering streaming mode");
    } else {
        return Err(format!("Unexpected PSYNC response: {psync_resp}"));
    }

    // Step 5: Stream commands from master and apply them
    let (pubsub_tx, _pubsub_rx): (mpsc::UnboundedSender<RespValue>, _) = mpsc::unbounded_channel();

    let mut ack_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Ok(());
            }
            result = stream.read_buf(&mut buf) => {
                match result {
                    Ok(0) => return Err("Master disconnected".to_string()),
                    Ok(_) => {
                        // Parse and apply commands
                        while let Ok(Some(value)) = RespParser::parse(&mut buf) {
                            apply_command(
                                value,
                                store,
                                config,
                                pubsub,
                                &pubsub_tx,
                                key_watcher,
                                script_cache,
                                repl_state,
                            ).await;
                        }
                    }
                    Err(e) => return Err(format!("Read error: {e}")),
                }
            }
            _ = ack_interval.tick() => {
                // Send REPLCONF ACK
                let offset = {
                    let state = repl_state.read().await;
                    state.master_repl_offset
                };
                let ack_cmd = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                    offset.to_string().len(),
                    offset
                );
                if stream.write_all(ack_cmd.as_bytes()).await.is_err() {
                    return Err("Failed to send ACK".to_string());
                }
            }
        }
    }
}

/// Apply a replicated command to the local store.
#[allow(clippy::too_many_arguments)]
async fn apply_command(
    value: RespValue,
    store: &SharedStore,
    config: &SharedConfig,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
    repl_state: &SharedReplicationState,
) {
    let items = match &value {
        RespValue::Array(Some(items)) if !items.is_empty() => items,
        _ => return,
    };

    let cmd_name = match items[0].to_string_lossy() {
        Some(name) => name.to_uppercase(),
        None => return,
    };

    // Skip PING commands from master (keepalive)
    if cmd_name == "PING" {
        return;
    }

    // Track offset: add the serialized size of this command
    let cmd_bytes = value.serialize();
    {
        let mut state = repl_state.write().await;
        state.master_repl_offset += cmd_bytes.len() as i64;
    }

    let args = &items[1..];

    // Use a temporary client state for replicated commands
    let mut client = ClientState::new();
    client.authenticated = true;
    client.is_replication_client = true;

    // Handle SELECT specially to track db_index
    if cmd_name == "SELECT"
        && let Some(db_str) = args.first().and_then(|a| a.to_string_lossy())
        && let Ok(db_idx) = db_str.parse::<usize>()
    {
        client.db_index = db_idx;
    }

    let _response = command::dispatch(
        &cmd_name,
        args,
        store,
        config,
        &mut client,
        pubsub,
        pubsub_tx,
        key_watcher,
        script_cache,
    )
    .await;
}

/// Receive an RDB bulk transfer from master.
async fn receive_rdb(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    store: &SharedStore,
    config: &SharedConfig,
    cancel: &CancellationToken,
) -> Result<(), String> {
    // Read the bulk string header: $<len>\r\n
    let header = read_line(stream, buf, cancel)
        .await
        .map_err(|e| format!("RDB header read failed: {e}"))?;

    if !header.starts_with('$') {
        return Err(format!("Expected bulk string for RDB, got: {header}"));
    }

    let rdb_len: usize = header[1..]
        .trim()
        .parse()
        .map_err(|_| "Invalid RDB length".to_string())?;

    // Read the RDB data
    let mut rdb_data = vec![0u8; rdb_len];
    let mut read = 0;

    // First consume any data already in buf
    let from_buf = std::cmp::min(buf.len(), rdb_len);
    if from_buf > 0 {
        rdb_data[..from_buf].copy_from_slice(&buf[..from_buf]);
        buf.advance(from_buf);
        read = from_buf;
    }

    while read < rdb_len {
        tokio::select! {
            result = stream.read(&mut rdb_data[read..]) => {
                match result {
                    Ok(0) => return Err("Master disconnected during RDB transfer".to_string()),
                    Ok(n) => read += n,
                    Err(e) => return Err(format!("RDB read error: {e}")),
                }
            }
            _ = cancel.cancelled() => {
                return Err("Cancelled during RDB transfer".to_string());
            }
        }
    }

    // Load the RDB data
    let num_dbs = {
        let cfg = config.read().await;
        cfg.databases
    };

    let new_store = rdb::load_from_reader(&mut rdb_data.as_slice(), num_dbs)
        .map_err(|e| format!("Failed to load RDB: {e}"))?;

    // Replace the store contents
    {
        let mut store_guard = store.write().await;
        *store_guard = new_store;
    }

    info!("Loaded RDB from master ({rdb_len} bytes)");
    Ok(())
}

/// Send a command as a RESP array.
async fn send_command(stream: &mut TcpStream, args: &[&str]) -> Result<(), std::io::Error> {
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    stream.write_all(cmd.as_bytes()).await
}

/// Read a single RESP response.
async fn read_response(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    cancel: &CancellationToken,
) -> Result<RespValue, String> {
    loop {
        if let Ok(Some(value)) = RespParser::parse(buf) {
            return Ok(value);
        }

        tokio::select! {
            result = stream.read_buf(buf) => {
                match result {
                    Ok(0) => return Err("Connection closed".to_string()),
                    Ok(_) => {}
                    Err(e) => return Err(format!("Read error: {e}")),
                }
            }
            _ = cancel.cancelled() => {
                return Err("Cancelled".to_string());
            }
        }
    }
}

/// Read a single line (up to \r\n) from the stream.
async fn read_line(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    cancel: &CancellationToken,
) -> Result<String, String> {
    loop {
        // Check if we have a complete line in the buffer
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&buf[..pos]).to_string();
            buf.advance(pos + 2);
            return Ok(line);
        }

        tokio::select! {
            result = stream.read_buf(buf) => {
                match result {
                    Ok(0) => return Err("Connection closed".to_string()),
                    Ok(_) => {}
                    Err(e) => return Err(format!("Read error: {e}")),
                }
            }
            _ = cancel.cancelled() => {
                return Err("Cancelled".to_string());
            }
        }
    }
}

use bytes::Buf;
