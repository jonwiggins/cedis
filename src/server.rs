use crate::command;
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::persistence::aof::SharedAofWriter;
use crate::pubsub::{PubSubReceiver, SharedPubSub};
use crate::resp::{RespParser, RespValue};
use crate::store::SharedStore;
use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info};

pub async fn run_server(
    store: SharedStore,
    config: SharedConfig,
    pubsub: SharedPubSub,
    aof: SharedAofWriter,
) -> std::io::Result<()> {
    let (bind, port) = {
        let cfg = config.read().await;
        (cfg.bind.clone(), cfg.port)
    };

    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("Cedis server listening on {addr}");

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

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, store, config, pubsub, aof).await {
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

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
    config: SharedConfig,
    pubsub: SharedPubSub,
    aof: SharedAofWriter,
) -> std::io::Result<()> {
    let mut client = ClientState::new();
    let mut buf = BytesMut::with_capacity(4096);

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
                Ok(Some(value)) => {
                    let response = process_command(
                        value,
                        &store,
                        &config,
                        &mut client,
                        &pubsub,
                        &pubsub_tx,
                        &aof,
                    )
                    .await;

                    let serialized = response.serialize();
                    stream.write_all(&serialized).await?;

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

        // Wait for data from either TCP or pub/sub, with optional timeout
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
                    Ok(_) => {} // Got data, loop back to parse
                    Err(e) => {
                        cleanup_client(&pubsub, &client).await;
                        return Err(e);
                    }
                }
            }
            Some(msg) = pubsub_rx.recv() => {
                // Forward pub/sub message to the client
                stream.write_all(&msg.serialize()).await?;
            }
        }
    }
}

/// Commands that are considered writes and should be logged to AOF.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "APPEND"
            | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT"
            | "SETRANGE" | "GETSET" | "GETDEL"
            | "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
            | "RENAME" | "RENAMENX"
            | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LSET" | "LINSERT" | "LREM" | "LTRIM"
            | "RPOPLPUSH" | "LMOVE" | "LMPOP"
            | "HSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "HSETNX" | "HMSET"
            | "SADD" | "SREM" | "SPOP" | "SMOVE"
            | "ZADD" | "ZREM" | "ZINCRBY" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZPOPMIN" | "ZPOPMAX"
            | "FLUSHDB" | "FLUSHALL" | "SWAPDB" | "SELECT"
    )
}

async fn process_command(
    value: RespValue,
    store: &SharedStore,
    config: &SharedConfig,
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    aof: &SharedAofWriter,
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

    // Check authentication
    if !client.authenticated && cmd_name != "AUTH" && cmd_name != "QUIT" && cmd_name != "HELLO" {
        return RespValue::error("NOAUTH Authentication required.");
    }

    // In subscribe mode, only allow certain commands
    if client.in_subscribe_mode() {
        match cmd_name.as_str() {
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET" => {}
            _ => {
                return RespValue::error(format!(
                    "ERR Can't execute '{cmd_name}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                ));
            }
        }
    }

    // Log write commands to AOF before executing
    if is_write_command(&cmd_name) {
        let mut aof = aof.lock().await;
        if aof.is_active() {
            let _ = aof.log_command(&cmd_name, args);
        }
    }

    command::dispatch(&cmd_name, args, store, config, client, pubsub, pubsub_tx).await
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
