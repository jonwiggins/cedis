use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;

pub fn cmd_ping(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        RespValue::SimpleString("PONG".to_string())
    } else if args.len() == 1 {
        if let Some(msg) = args[0].as_str() {
            RespValue::bulk_string(msg.to_vec())
        } else {
            RespValue::SimpleString("PONG".to_string())
        }
    } else {
        wrong_arg_count("ping")
    }
}

pub fn cmd_echo(args: &[RespValue]) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("echo");
    }
    if let Some(msg) = args[0].as_str() {
        RespValue::bulk_string(msg.to_vec())
    } else {
        RespValue::null_bulk_string()
    }
}

pub fn cmd_quit(client: &mut ClientState) -> RespValue {
    client.should_close = true;
    RespValue::ok()
}

pub async fn cmd_select(
    args: &[RespValue],
    client: &mut ClientState,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("select");
    }
    let db_index = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let cfg = config.read().await;
    if db_index >= cfg.databases {
        return RespValue::error("ERR DB index is out of range");
    }
    drop(cfg);

    client.db_index = db_index;
    RespValue::ok()
}

pub async fn cmd_auth(
    args: &[RespValue],
    client: &mut ClientState,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("auth");
    }

    let password = match arg_to_string(&args[0]) {
        Some(p) => p,
        None => return RespValue::error("ERR invalid password"),
    };

    let cfg = config.read().await;
    match &cfg.requirepass {
        Some(pass) if pass == &password => {
            client.authenticated = true;
            RespValue::ok()
        }
        Some(_) => RespValue::error("WRONGPASS invalid username-password pair or user is disabled."),
        None => RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"),
    }
}

pub async fn cmd_dbsize(store: &SharedStore, client: &ClientState) -> RespValue {
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(db.dbsize() as i64)
}

pub async fn cmd_flushdb(store: &SharedStore, client: &ClientState) -> RespValue {
    let mut store = store.write().await;
    store.db(client.db_index).flush();
    RespValue::ok()
}

pub async fn cmd_flushall(store: &SharedStore) -> RespValue {
    let mut store = store.write().await;
    store.flush_all();
    RespValue::ok()
}

pub async fn cmd_swapdb(
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("swapdb");
    }
    let a = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let b = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let cfg = config.read().await;
    if a >= cfg.databases || b >= cfg.databases {
        return RespValue::error("ERR invalid DB index");
    }
    drop(cfg);

    let mut store = store.write().await;
    if store.swap_db(a, b) {
        RespValue::ok()
    } else {
        RespValue::error("ERR invalid DB index")
    }
}

pub async fn cmd_info(
    _args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
) -> RespValue {
    let cfg = config.read().await;
    let store = store.read().await;

    let mut info = String::new();

    // Server section
    info.push_str("# Server\r\n");
    info.push_str("redis_version:7.0.0\r\n");
    info.push_str("cedis_version:0.1.0\r\n");
    info.push_str(&format!("tcp_port:{}\r\n", cfg.port));
    info.push_str("config_file:\r\n");

    // Clients section
    info.push_str("\r\n# Clients\r\n");
    info.push_str("connected_clients:1\r\n");

    // Memory section
    info.push_str("\r\n# Memory\r\n");
    info.push_str("used_memory:0\r\n");
    info.push_str("used_memory_human:0B\r\n");

    // Stats section
    info.push_str("\r\n# Stats\r\n");
    info.push_str("total_connections_received:0\r\n");
    info.push_str("total_commands_processed:0\r\n");

    // Replication section
    info.push_str("\r\n# Replication\r\n");
    info.push_str("role:master\r\n");
    info.push_str("connected_slaves:0\r\n");

    // Keyspace section
    info.push_str("\r\n# Keyspace\r\n");
    for (i, db) in store.databases.iter().enumerate() {
        let size = db.dbsize();
        if size > 0 {
            let expires = db.expires_count();
            info.push_str(&format!("db{i}:keys={size},expires={expires},avg_ttl=0\r\n"));
        }
    }

    RespValue::bulk_string(info.into_bytes())
}

pub async fn cmd_config(args: &[RespValue], config: &SharedConfig) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("config");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid config subcommand"),
    };

    match subcmd.as_str() {
        "GET" => {
            if args.len() != 2 {
                return wrong_arg_count("config|get");
            }
            let pattern = match arg_to_string(&args[1]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid parameter"),
            };

            let cfg = config.read().await;
            let params = [
                "bind", "port", "databases", "requirepass", "timeout",
                "tcp-keepalive", "hz", "loglevel", "dbfilename", "dir",
                "appendonly", "appendfsync", "maxmemory", "maxmemory-policy",
                "save",
            ];

            let mut result = Vec::new();
            for param in &params {
                if crate::glob::glob_match(&pattern, param) {
                    if let Some(val) = cfg.get(param) {
                        result.push(RespValue::bulk_string(param.as_bytes().to_vec()));
                        result.push(RespValue::bulk_string(val.into_bytes()));
                    }
                }
            }
            RespValue::array(result)
        }
        "SET" => {
            if args.len() != 3 {
                return wrong_arg_count("config|set");
            }
            let param = match arg_to_string(&args[1]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid parameter"),
            };
            let value = match arg_to_string(&args[2]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid value"),
            };

            let mut cfg = config.write().await;
            match cfg.set(&param, &value) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(format!("ERR {e}")),
            }
        }
        "RESETSTAT" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for CONFIG {subcmd}"
        )),
    }
}

pub fn cmd_time() -> RespValue {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before UNIX epoch");
    let secs = now.as_secs().to_string();
    let micros = (now.subsec_micros()).to_string();
    RespValue::array(vec![
        RespValue::bulk_string(secs.into_bytes()),
        RespValue::bulk_string(micros.into_bytes()),
    ])
}

pub fn cmd_command(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        // Return basic command info
        return RespValue::array(vec![]);
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "COUNT" => RespValue::integer(100), // approximate
        "DOCS" => RespValue::array(vec![]),
        "INFO" => RespValue::array(vec![]),
        _ => RespValue::error(format!("ERR unknown subcommand '{subcmd}'")),
    }
}

pub fn cmd_client(args: &[RespValue], client: &mut ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("client");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "SETNAME" => {
            if args.len() != 2 {
                return wrong_arg_count("client|setname");
            }
            client.name = arg_to_string(&args[1]);
            RespValue::ok()
        }
        "GETNAME" => match &client.name {
            Some(name) => RespValue::bulk_string(name.as_bytes().to_vec()),
            None => RespValue::null_bulk_string(),
        },
        "ID" => RespValue::integer(client.id as i64),
        "LIST" => {
            let info = format!(
                "id={} addr=127.0.0.1 fd=0 name={} db={} cmd=client\n",
                client.id,
                client.name.as_deref().unwrap_or(""),
                client.db_index,
            );
            RespValue::bulk_string(info.into_bytes())
        }
        "INFO" => {
            let info = format!(
                "id={}\r\naddr=127.0.0.1\r\nname={}\r\ndb={}\r\n",
                client.id,
                client.name.as_deref().unwrap_or(""),
                client.db_index,
            );
            RespValue::bulk_string(info.into_bytes())
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for CLIENT {subcmd}"
        )),
    }
}

pub async fn cmd_debug(args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("debug");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "SLEEP" => {
            if args.len() != 2 {
                return wrong_arg_count("debug|sleep");
            }
            let secs: f64 = match arg_to_string(&args[1]).and_then(|s| s.parse().ok()) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid sleep time"),
            };
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            RespValue::ok()
        }
        "SET-ACTIVE-EXPIRE" => RespValue::ok(),
        _ => RespValue::error(format!("ERR Unknown DEBUG subcommand '{subcmd}'")),
    }
}

pub fn cmd_reset(client: &mut ClientState) -> RespValue {
    client.db_index = 0;
    client.in_multi = false;
    client.multi_queue.clear();
    client.watched_keys.clear();
    client.name = None;
    RespValue::SimpleString("RESET".to_string())
}
