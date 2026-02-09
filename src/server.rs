use crate::command;
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::resp::{RespParser, RespValue};
use crate::store::SharedStore;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

pub async fn run_server(store: SharedStore, config: SharedConfig) -> std::io::Result<()> {
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

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        debug!("New connection from {peer_addr}");

        let store = store.clone();
        let config = config.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store, config).await {
                debug!("Connection error from {peer_addr}: {e}");
            }
            debug!("Connection closed: {peer_addr}");
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
    config: SharedConfig,
) -> std::io::Result<()> {
    let mut client = ClientState::new();
    let mut buf = BytesMut::with_capacity(4096);

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
                    let response = process_command(value, &store, &config, &mut client).await;

                    // Write response(s)
                    let serialized = response.serialize();
                    stream.write_all(&serialized).await?;

                    if client.should_close {
                        return Ok(());
                    }
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    let err_resp = RespValue::error(format!("ERR Protocol error: {e}"));
                    stream.write_all(&err_resp.serialize()).await?;
                    return Ok(());
                }
            }
        }

        // Read more data from the socket
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            // Connection closed
            return Ok(());
        }
    }
}

async fn process_command(
    value: RespValue,
    store: &SharedStore,
    config: &SharedConfig,
    client: &mut ClientState,
) -> RespValue {
    // Extract command array
    let items = match value {
        RespValue::Array(Some(items)) if !items.is_empty() => items,
        _ => return RespValue::error("ERR invalid command format"),
    };

    // Get command name
    let cmd_name = match items[0].to_string_lossy() {
        Some(name) => name.to_uppercase(),
        None => return RespValue::error("ERR invalid command name"),
    };

    let args = &items[1..];

    // Check authentication
    if !client.authenticated && cmd_name != "AUTH" && cmd_name != "QUIT" && cmd_name != "HELLO" {
        return RespValue::error("NOAUTH Authentication required.");
    }

    command::dispatch(&cmd_name, args, store, config, client).await
}

/// Background task that periodically expires keys.
async fn active_expiration_loop(store: SharedStore, config: SharedConfig) {
    loop {
        let hz = {
            let cfg = config.read().await;
            cfg.hz
        };
        let interval = std::time::Duration::from_millis(1000 / hz.max(1));

        tokio::time::sleep(interval).await;

        let mut store = store.write().await;
        store.active_expire_cycle();
    }
}
