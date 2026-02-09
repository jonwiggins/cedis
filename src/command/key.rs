use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::now_millis;

pub async fn cmd_del(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("del");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg) {
            if db.del(&key) {
                count += 1;
            }
        }
    }

    RespValue::integer(count)
}

pub async fn cmd_exists(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("exists");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut count = 0i64;

    for arg in args {
        if let Some(key) = arg_to_string(arg) {
            if db.exists(&key) {
                count += 1;
            }
        }
    }

    RespValue::integer(count)
}

pub async fn cmd_expire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("expire");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let seconds = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if seconds <= 0 {
        // Negative or zero TTL means delete
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + (seconds as u64) * 1000;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_pexpire(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("pexpire");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let millis = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if millis <= 0 {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        return RespValue::integer(if db.del(&key) { 1 } else { 0 });
    }

    let expires_at = now_millis() + millis as u64;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_expireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("expireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let expires_at = timestamp * 1000;
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, expires_at) { 1 } else { 0 })
}

pub async fn cmd_pexpireat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("pexpireat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let timestamp = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.set_expiry(&key, timestamp) { 1 } else { 0 })
}

pub async fn cmd_ttl(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("ttl");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => RespValue::integer(entry.ttl_seconds()),
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_pttl(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("pttl");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(-2),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => RespValue::integer(entry.ttl_millis()),
        None => RespValue::integer(-2),
    }
}

pub async fn cmd_persist(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("persist");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    RespValue::integer(if db.persist(&key) { 1 } else { 0 })
}

pub async fn cmd_type(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("type");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::SimpleString("none".to_string()),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.key_type(&key) {
        Some(t) => RespValue::SimpleString(t.to_string()),
        None => RespValue::SimpleString("none".to_string()),
    }
}

pub async fn cmd_rename(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("rename");
    }
    let old = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR no such key"),
    };
    let new = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if db.rename(&old, &new) {
        RespValue::ok()
    } else {
        RespValue::error("ERR no such key")
    }
}

pub async fn cmd_renamenx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("renamenx");
    }
    let old = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR no such key"),
    };
    let new = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if !db.exists(&old) {
        return RespValue::error("ERR no such key");
    }
    if db.exists(&new) {
        return RespValue::integer(0);
    }

    db.rename(&old, &new);
    RespValue::integer(1)
}

pub async fn cmd_keys(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("keys");
    }
    let pattern = match arg_to_string(&args[0]) {
        Some(p) => p,
        None => return RespValue::array(vec![]),
    };

    let store = store.read().await;
    let db = &store.databases[client.db_index];

    let keys = db.keys(&pattern);
    let items: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::bulk_string(k.into_bytes()))
        .collect();
    RespValue::array(items)
}

pub async fn cmd_scan(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("scan");
    }
    let cursor = match arg_to_i64(&args[0]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern = None;
    let mut count = 10usize;
    let mut i = 1;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = arg_to_string(args.get(i).unwrap_or(&RespValue::null_bulk_string()));
            }
            "COUNT" => {
                i += 1;
                if let Some(c) = arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    count = c as usize;
                }
            }
            "TYPE" => {
                i += 1;
                // TYPE filter - not fully implemented yet
            }
            _ => {}
        }
        i += 1;
    }

    let store = store.read().await;
    let db = &store.databases[client.db_index];

    let (next_cursor, keys) = db.scan(cursor, pattern.as_deref(), count);

    let key_values: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::bulk_string(k.into_bytes()))
        .collect();

    RespValue::array(vec![
        RespValue::bulk_string(next_cursor.to_string().into_bytes()),
        RespValue::array(key_values),
    ])
}

pub async fn cmd_randomkey(store: &SharedStore, client: &ClientState) -> RespValue {
    let store = store.read().await;
    let db = &store.databases[client.db_index];

    match db.random_key() {
        Some(key) => RespValue::bulk_string(key.into_bytes()),
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_object(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("object");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match subcmd.as_str() {
        "ENCODING" => {
            if args.len() != 2 {
                return wrong_arg_count("object|encoding");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };

            let mut store = store.write().await;
            let db = store.db(client.db_index);

            match db.get(&key) {
                Some(entry) => {
                    let encoding = match &entry.value {
                        crate::types::RedisValue::String(s) => {
                            if s.as_i64().is_some() {
                                "int"
                            } else {
                                "embstr"
                            }
                        }
                        crate::types::RedisValue::List(_) => "listpack",
                        crate::types::RedisValue::Hash(_) => "listpack",
                        crate::types::RedisValue::Set(_) => "listpack",
                        crate::types::RedisValue::SortedSet(_) => "listpack",
                    };
                    RespValue::bulk_string(encoding.as_bytes().to_vec())
                }
                None => RespValue::null_bulk_string(),
            }
        }
        "REFCOUNT" => {
            if args.len() != 2 {
                return wrong_arg_count("object|refcount");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::null_bulk_string(),
            };
            let mut store = store.write().await;
            let db = store.db(client.db_index);
            if db.exists(&key) {
                RespValue::integer(1)
            } else {
                RespValue::null_bulk_string()
            }
        }
        "IDLETIME" => {
            if args.len() != 2 {
                return wrong_arg_count("object|idletime");
            }
            RespValue::integer(0)
        }
        "HELP" => {
            let help = vec![
                RespValue::bulk_string(b"OBJECT ENCODING <key>".to_vec()),
                RespValue::bulk_string(b"OBJECT REFCOUNT <key>".to_vec()),
                RespValue::bulk_string(b"OBJECT IDLETIME <key>".to_vec()),
                RespValue::bulk_string(b"OBJECT HELP".to_vec()),
            ];
            RespValue::array(help)
        }
        _ => RespValue::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for OBJECT {subcmd}"
        )),
    }
}

pub fn cmd_dump(_args: &[RespValue]) -> RespValue {
    RespValue::error("ERR DUMP not yet implemented")
}

pub fn cmd_restore(_args: &[RespValue]) -> RespValue {
    RespValue::error("ERR RESTORE not yet implemented")
}
