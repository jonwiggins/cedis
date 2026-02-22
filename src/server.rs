use crate::command;
use crate::config::SharedConfig;
use crate::connection::{ClientState, MonitorSender, new_monitor_sender};
use crate::keywatcher::{KeyWatcher, SharedKeyWatcher};
use crate::persistence::aof::SharedAofWriter;
use crate::pubsub::{PubSubReceiver, SharedPubSub};
use crate::replication::{ReplicationRole, SharedReplicationState};
use crate::resp::{RespParser, RespValue};
use crate::scripting::ScriptCache;
use crate::store::SharedStore;
use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, info};

type SharedChangeCounter = Arc<AtomicU64>;

pub async fn run_server(
    store: SharedStore,
    config: SharedConfig,
    pubsub: SharedPubSub,
    aof: SharedAofWriter,
    repl_state: SharedReplicationState,
) -> std::io::Result<()> {
    let (bind, port) = {
        let cfg = config.read().await;
        (cfg.bind.clone(), cfg.port)
    };

    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("Cedis server listening on {addr}");

    let change_counter: SharedChangeCounter = Arc::new(AtomicU64::new(0));
    let key_watcher: SharedKeyWatcher = Arc::new(RwLock::new(KeyWatcher::new()));
    let script_cache = ScriptCache::new();
    let monitor_tx = new_monitor_sender();

    // Spawn active expiration background task
    let store_clone = store.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        active_expiration_loop(store_clone, config_clone).await;
    });

    // Spawn AOF fsync background task
    let aof_clone = aof.clone();
    tokio::spawn(async move {
        aof_fsync_loop(aof_clone).await;
    });

    // Spawn auto-save background task
    let store_clone = store.clone();
    let config_clone = config.clone();
    let changes_clone = change_counter.clone();
    tokio::spawn(async move {
        auto_save_loop(store_clone, config_clone, changes_clone).await;
    });

    // Spawn memory eviction background task
    let store_clone = store.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        memory_eviction_loop(store_clone, config_clone).await;
    });

    // If configured as replica, start sync loop
    {
        let cfg = config.read().await;
        if let Some((ref host, port)) = cfg.replicaof {
            let cancel = tokio_util::sync::CancellationToken::new();
            {
                let mut state = repl_state.write().await;
                state.role = ReplicationRole::Replica;
                state.master_host = Some(host.clone());
                state.master_port = Some(port);
                state.cancel = Some(cancel.clone());
            }

            let host = host.clone();
            let store_c = store.clone();
            let config_c = config.clone();
            let repl_c = repl_state.clone();
            let pubsub_c = pubsub.clone();
            let kw_c = key_watcher.clone();
            let sc_c = script_cache.clone();

            tokio::spawn(async move {
                crate::replication::replica::replica_sync_loop(
                    host, port, store_c, config_c, repl_c, pubsub_c, kw_c, sc_c, cancel,
                )
                .await;
            });
        }
    }

    // Accept loop with graceful shutdown on ctrl-c
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = result?;
                debug!("New connection from {peer_addr}");

                let store = store.clone();
                let config = config.clone();
                let pubsub = pubsub.clone();
                let aof = aof.clone();
                let change_counter = change_counter.clone();
                let key_watcher = key_watcher.clone();
                let script_cache = script_cache.clone();
                let monitor_tx = monitor_tx.clone();
                let repl_state = repl_state.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, store, config, pubsub, aof, change_counter, key_watcher, script_cache, monitor_tx, repl_state).await {
                        debug!("Connection error from {peer_addr}: {e}");
                    }
                    debug!("Connection closed: {peer_addr}");
                });
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
                // Flush AOF
                let mut aof = aof.lock().await;
                let _ = aof.flush();
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
    config: SharedConfig,
    pubsub: SharedPubSub,
    aof: SharedAofWriter,
    change_counter: SharedChangeCounter,
    key_watcher: SharedKeyWatcher,
    script_cache: ScriptCache,
    monitor_tx: MonitorSender,
    repl_state: SharedReplicationState,
) -> std::io::Result<()> {
    let mut client = ClientState::new();
    let mut buf = BytesMut::with_capacity(4096);
    let mut monitor_rx: Option<tokio::sync::broadcast::Receiver<String>> = None;

    // Create pub/sub receiver channel
    let (pubsub_tx, mut pubsub_rx): (mpsc::UnboundedSender<RespValue>, PubSubReceiver) =
        mpsc::unbounded_channel();

    // Check if auth is required
    {
        let cfg = config.read().await;
        if cfg.requirepass.is_none() {
            client.authenticated = true;
        }
    }

    loop {
        // Try to parse any complete commands in the buffer first
        loop {
            match RespParser::parse(&mut buf) {
                Ok(Some(RespValue::Array(None))) => {
                    // Silently ignore null arrays (e.g. *-10)
                    continue;
                }
                Ok(Some(RespValue::Array(Some(ref items)))) if items.is_empty() => {
                    // Silently ignore empty arrays (e.g. *0)
                    continue;
                }
                Ok(Some(value)) => {
                    // Check if this is a PSYNC/SYNC command - needs to take over connection
                    let is_psync = matches!(
                        &value,
                        RespValue::Array(Some(items)) if !items.is_empty()
                            && matches!(
                                items[0].to_string_lossy().map(|s| s.to_uppercase()).as_deref(),
                                Some("PSYNC") | Some("SYNC")
                            )
                    );

                    if is_psync && let RespValue::Array(Some(ref items)) = value {
                        let (replid, offset) = if items.len() >= 3 {
                            let rid = items[1].to_string_lossy().unwrap_or("?".to_string());
                            let off: i64 = items[2]
                                .to_string_lossy()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(-1);
                            (rid, off)
                        } else {
                            ("?".to_string(), -1i64)
                        };

                        let backlog_size = {
                            let cfg = config.read().await;
                            cfg.repl_backlog_size
                        };

                        // Hand off to PSYNC handler - this takes over the connection
                        crate::replication::master::handle_psync(
                            stream,
                            replid,
                            offset,
                            &store,
                            &repl_state,
                            backlog_size,
                        )
                        .await;
                        return Ok(());
                    }

                    // Check if this is a MONITOR command
                    let is_monitor = matches!(
                        &value,
                        RespValue::Array(Some(items)) if !items.is_empty()
                            && items[0].to_string_lossy().map(|s| s.to_uppercase()) == Some("MONITOR".to_string())
                    );

                    let response = process_command(
                        value,
                        &store,
                        &config,
                        &mut client,
                        &pubsub,
                        &pubsub_tx,
                        &aof,
                        &change_counter,
                        &key_watcher,
                        &script_cache,
                        &monitor_tx,
                        &repl_state,
                    )
                    .await;

                    let serialized = response.serialize();
                    stream.write_all(&serialized).await?;

                    if is_monitor && client.in_monitor {
                        monitor_rx = Some(monitor_tx.subscribe());
                    }

                    if client.should_close {
                        cleanup_client(&pubsub, &client).await;
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let err_resp = RespValue::error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&err_resp.serialize()).await?;
                    cleanup_client(&pubsub, &client).await;
                    return Ok(());
                }
            }
        }

        // Get timeout from config
        let timeout_duration = {
            let cfg = config.read().await;
            if cfg.timeout > 0 {
                Some(Duration::from_secs(cfg.timeout))
            } else {
                None
            }
        };

        // Wait for data from either TCP, pub/sub, or monitor, with optional timeout
        tokio::select! {
            result = async {
                if let Some(dur) = timeout_duration {
                    match tokio::time::timeout(dur, stream.read_buf(&mut buf)).await {
                        Ok(result) => result,
                        Err(_) => Ok(0), // Timeout => treat as disconnect
                    }
                } else {
                    stream.read_buf(&mut buf).await
                }
            } => {
                match result {
                    Ok(0) => {
                        cleanup_client(&pubsub, &client).await;
                        return Ok(());
                    }
                    Ok(_) => {
                        if buf.len() > 64 * 1024 {
                            let has_early_crlf = buf[..std::cmp::min(buf.len(), 64 * 1024)]
                                .windows(2)
                                .any(|w| w == b"\r\n");
                            if !has_early_crlf {
                                let err_resp = RespValue::error("ERR Protocol error: too big inline request");
                                stream.write_all(&err_resp.serialize()).await?;
                                cleanup_client(&pubsub, &client).await;
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        cleanup_client(&pubsub, &client).await;
                        return Err(e);
                    }
                }
            }
            Some(msg) = pubsub_rx.recv() => {
                stream.write_all(&msg.serialize()).await?;
            }
            msg = async {
                if let Some(ref mut rx) = monitor_rx {
                    rx.recv().await.ok()
                } else {
                    std::future::pending::<Option<String>>().await
                }
            } => {
                if let Some(line) = msg {
                    let resp = RespValue::SimpleString(line);
                    stream.write_all(&resp.serialize()).await?;
                }
            }
        }
    }
}

/// Commands that are considered writes and should be logged to AOF.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "MSET"
            | "MSETNX"
            | "APPEND"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "SETRANGE"
            | "GETSET"
            | "GETDEL"
            | "DEL"
            | "UNLINK"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "RENAME"
            | "RENAMENX"
            | "LPUSH"
            | "RPUSH"
            | "LPUSHX"
            | "RPUSHX"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "LINSERT"
            | "LREM"
            | "LTRIM"
            | "RPOPLPUSH"
            | "LMOVE"
            | "LMPOP"
            | "BLPOP"
            | "BRPOP"
            | "BLMOVE"
            | "BLMPOP"
            | "HSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HSETNX"
            | "HMSET"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "SMOVE"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZUNIONSTORE"
            | "ZINTERSTORE"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "SETBIT"
            | "PFADD"
            | "PFMERGE"
            | "XADD"
            | "XTRIM"
            | "XDEL"
            | "XGROUP"
            | "XACK"
            | "XCLAIM"
            | "XAUTOCLAIM"
            | "XREADGROUP"
            | "BITOP"
            | "GEOADD"
            | "COPY"
            | "FLUSHDB"
            | "FLUSHALL"
            | "SWAPDB"
            | "SELECT"
            | "EVAL"
            | "EVALSHA"
    )
}

#[allow(clippy::too_many_arguments)]
async fn process_command(
    value: RespValue,
    store: &SharedStore,
    config: &SharedConfig,
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    aof: &SharedAofWriter,
    change_counter: &SharedChangeCounter,
    key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
    monitor_tx: &MonitorSender,
    repl_state: &SharedReplicationState,
) -> RespValue {
    let items = match value {
        RespValue::Array(Some(items)) if !items.is_empty() => items,
        _ => return RespValue::error("ERR invalid command format"),
    };

    let cmd_name = match items[0].to_string_lossy() {
        Some(name) => name.to_uppercase(),
        None => return RespValue::error("ERR invalid command name"),
    };

    let args = &items[1..];

    // Broadcast to MONITOR clients (skip MONITOR command itself and AUTH for security)
    if cmd_name != "MONITOR" && cmd_name != "AUTH" && monitor_tx.receiver_count() > 0 {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = timestamp.as_secs();
        let micros = timestamp.subsec_micros();
        let args_str: Vec<String> = args
            .iter()
            .filter_map(|a| a.to_string_lossy())
            .map(|s| format!("\"{}\"", s))
            .collect();
        let line = format!(
            "{secs}.{micros:06} [{}] \"{}\" {}",
            client.db_index,
            cmd_name.to_lowercase(),
            args_str.join(" ")
        );
        let _ = monitor_tx.send(line);
    }

    // Check authentication
    if !client.authenticated && cmd_name != "AUTH" && cmd_name != "QUIT" && cmd_name != "HELLO" {
        return RespValue::error("NOAUTH Authentication required.");
    }

    // Read-only enforcement for replicas
    if !client.is_replication_client {
        let is_replica = {
            let state = repl_state.read().await;
            state.role == ReplicationRole::Replica
        };
        let is_readonly = {
            let cfg = config.read().await;
            cfg.replica_read_only
        };
        if is_replica && is_readonly && is_write_command(&cmd_name) {
            return RespValue::error("READONLY You can't write against a read only replica.");
        }
    }

    // In subscribe mode, only allow certain commands
    if client.in_subscribe_mode() {
        match cmd_name.as_str() {
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT"
            | "RESET" => {}
            _ => {
                return RespValue::error(format!(
                    "ERR Can't execute '{cmd_name}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                ));
            }
        }
    }

    // In subscribe mode, PING returns pub/sub-style array
    if client.in_subscribe_mode() && cmd_name == "PING" {
        let msg = if !args.is_empty() {
            args[0].to_string_lossy().unwrap_or_default()
        } else {
            String::new()
        };
        return RespValue::array(vec![
            RespValue::bulk_string(b"pong".to_vec()),
            RespValue::bulk_string(msg.into_bytes()),
        ]);
    }

    // Log write commands to AOF before executing (skip for replication clients)
    let is_write = is_write_command(&cmd_name);
    if is_write && !client.is_replication_client {
        let mut aof = aof.lock().await;
        if aof.is_active() {
            let _ = aof.log_command(&cmd_name, args);
        }
        change_counter.fetch_add(1, Ordering::Relaxed);
    }

    let response = command::dispatch(
        &cmd_name,
        args,
        store,
        config,
        client,
        pubsub,
        pubsub_tx,
        key_watcher,
        script_cache,
    )
    .await;

    // Touch modified keys for WATCH support
    if is_write {
        let mut store_guard = store.write().await;
        let db = store_guard.db(client.db_index);
        match cmd_name.as_str() {
            "FLUSHDB" | "FLUSHALL" | "SWAPDB" => {
                db.touch_all();
            }
            "RENAME" | "RENAMENX" => {
                if let Some(k) = args.first().and_then(|a| a.to_string_lossy()) {
                    db.touch(&k);
                }
                if let Some(k) = args.get(1).and_then(|a| a.to_string_lossy()) {
                    db.touch(&k);
                }
            }
            "DEL" | "UNLINK" => {
                for a in args {
                    if let Some(k) = a.to_string_lossy() {
                        db.touch(&k);
                    }
                }
            }
            "MSET" | "MSETNX" => {
                for i in (0..args.len()).step_by(2) {
                    if let Some(k) = args[i].to_string_lossy() {
                        db.touch(&k);
                    }
                }
            }
            _ => {
                if let Some(k) = args.first().and_then(|a| a.to_string_lossy()) {
                    db.touch(&k);
                }
            }
        }
    }

    // Replicate write commands to connected replicas (skip for replication clients)
    if is_write && !client.is_replication_client {
        let mut state = repl_state.write().await;
        if state.role == ReplicationRole::Master && !state.replicas.is_empty() {
            // Serialize the command to RESP
            let mut cmd_parts = vec![items[0].clone()];
            cmd_parts.extend_from_slice(args);
            let resp_cmd = RespValue::Array(Some(cmd_parts));
            let serialized = resp_cmd.serialize();

            state.ensure_backlog({
                let cfg = config.read().await;
                cfg.repl_backlog_size
            });
            state.feed_backlog(&serialized);
            state.propagate_to_replicas(&serialized);
        }
    }

    response
}

async fn cleanup_client(pubsub: &SharedPubSub, client: &ClientState) {
    let mut ps = pubsub.write().await;
    ps.unsubscribe_all(client.id);
}

/// Background task that periodically expires keys.
async fn active_expiration_loop(store: SharedStore, config: SharedConfig) {
    loop {
        let hz = {
            let cfg = config.read().await;
            cfg.hz
        };
        let interval = Duration::from_millis(1000 / hz.max(1));

        tokio::time::sleep(interval).await;

        {
            let cfg = config.read().await;
            if !cfg.active_expire_enabled {
                continue;
            }
        }

        let mut store = store.write().await;
        store.active_expire_cycle();
    }
}

/// Background task that flushes AOF every second (for everysec policy).
async fn aof_fsync_loop(aof: SharedAofWriter) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut aof = aof.lock().await;
        let _ = aof.flush();
    }
}

/// Background task that periodically saves RDB snapshots based on save rules.
async fn auto_save_loop(store: SharedStore, config: SharedConfig, changes: SharedChangeCounter) {
    let mut last_save = std::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (save_rules, dir, dbfilename) = {
            let cfg = config.read().await;
            (
                cfg.save_rules.clone(),
                cfg.dir.clone(),
                cfg.dbfilename.clone(),
            )
        };
        let current_changes = changes.load(Ordering::Relaxed);
        let elapsed = last_save.elapsed().as_secs();

        let should_save = save_rules
            .iter()
            .any(|(secs, min_changes)| elapsed >= *secs && current_changes >= *min_changes);

        if should_save && current_changes > 0 {
            let store = store.read().await;
            let path = format!("{dir}/{dbfilename}");
            if let Err(e) = crate::persistence::rdb::save(&store, &path) {
                tracing::warn!("Auto-save failed: {e}");
            } else {
                tracing::info!("Auto-save completed ({current_changes} changes)");
            }
            drop(store);
            changes.store(0, Ordering::Relaxed);
            last_save = std::time::Instant::now();
        }
    }
}

/// Background task that evicts keys when memory usage exceeds maxmemory.
async fn memory_eviction_loop(store: SharedStore, config: SharedConfig) {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (maxmemory, policy) = {
            let cfg = config.read().await;
            (cfg.maxmemory, cfg.maxmemory_policy.clone())
        };
        if maxmemory == 0 {
            continue; // No limit set
        }
        let mut store = store.write().await;
        let used = store.estimated_memory();
        if used <= maxmemory as usize {
            continue;
        }
        // Evict keys until under limit or no more keys
        for _ in 0..10 {
            let evicted = match policy.as_str() {
                "allkeys-random" => store.databases.iter_mut().any(|db| db.evict_one_random()),
                "volatile-random" => store
                    .databases
                    .iter_mut()
                    .any(|db| db.evict_one_volatile_random()),
                "volatile-ttl" => store
                    .databases
                    .iter_mut()
                    .any(|db| db.evict_one_volatile_ttl()),
                _ => false, // noeviction - do nothing
            };
            if !evicted {
                break;
            }
            if store.estimated_memory() <= maxmemory as usize {
                break;
            }
        }
    }
}
