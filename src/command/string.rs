use crate::command::{arg_to_bytes, arg_to_f64, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::{Entry, now_millis};
use crate::types::RedisValue;
use crate::types::rstring::RedisString;

pub async fn cmd_get(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("get");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => RespValue::bulk_string(s.as_bytes().to_vec()),
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_set(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("set");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let value = match arg_to_bytes(&args[1]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    // Parse options: EX, PX, NX, XX, KEEPTTL, GET
    let mut ex: Option<u64> = None;
    let mut px: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get = false;

    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };
        match opt.as_str() {
            "EX" => {
                i += 1;
                ex = Some(match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n as u64,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                });
            }
            "PX" => {
                i += 1;
                px = Some(match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n as u64,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                });
            }
            "EXAT" => {
                i += 1;
                let ts = match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n as u64,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                px = Some(ts * 1000 - now_millis().max(1)); // Convert to relative ms
            }
            "PXAT" => {
                i += 1;
                let ts = match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n as u64,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                px = Some(ts - now_millis().min(ts)); // Convert to relative ms
            }
            "NX" => nx = true,
            "XX" => xx = true,
            "KEEPTTL" => keepttl = true,
            "GET" => get = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // GET option: return old value
    let old_value = if get {
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::String(s) => Some(RespValue::bulk_string(s.as_bytes().to_vec())),
                _ => return wrong_type_error(),
            },
            None => Some(RespValue::null_bulk_string()),
        }
    } else {
        None
    };

    // NX/XX checks
    let key_exists = db.exists(&key);
    if nx && key_exists {
        return old_value.unwrap_or(RespValue::null_bulk_string());
    }
    if xx && !key_exists {
        return old_value.unwrap_or(RespValue::null_bulk_string());
    }

    // Get old TTL if KEEPTTL
    let old_expiry = if keepttl {
        db.get(&key).and_then(|e| e.expires_at)
    } else {
        None
    };

    // Calculate expiry
    let expires_at = if let Some(seconds) = ex {
        Some(now_millis() + seconds * 1000)
    } else if let Some(millis) = px {
        Some(now_millis() + millis)
    } else if keepttl {
        old_expiry
    } else {
        None
    };

    let mut entry = Entry::new(RedisValue::String(RedisString::new(value)));
    entry.expires_at = expires_at;
    db.set(key, entry);

    old_value.unwrap_or(RespValue::ok())
}

pub async fn cmd_getset(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("getset");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let value = match arg_to_bytes(&args[1]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let old = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => RespValue::bulk_string(s.as_bytes().to_vec()),
            _ => return wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    };

    db.set(key, Entry::new(RedisValue::String(RedisString::new(value))));
    old
}

pub async fn cmd_mget(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("mget");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let mut results = Vec::with_capacity(args.len());

    for arg in args {
        let key = match arg_to_string(arg) {
            Some(k) => k,
            None => {
                results.push(RespValue::null_bulk_string());
                continue;
            }
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::String(s) => {
                    results.push(RespValue::bulk_string(s.as_bytes().to_vec()));
                }
                _ => results.push(RespValue::null_bulk_string()),
            },
            None => results.push(RespValue::null_bulk_string()),
        }
    }

    RespValue::array(results)
}

pub async fn cmd_mset(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return wrong_arg_count("mset");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    for pair in args.chunks(2) {
        let key = match arg_to_string(&pair[0]) {
            Some(k) => k,
            None => continue,
        };
        let value = match arg_to_bytes(&pair[1]) {
            Some(v) => v.to_vec(),
            None => continue,
        };
        db.set(key, Entry::new(RedisValue::String(RedisString::new(value))));
    }

    RespValue::ok()
}

pub async fn cmd_msetnx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return wrong_arg_count("msetnx");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Check if any key exists
    for pair in args.chunks(2) {
        let key = match arg_to_string(&pair[0]) {
            Some(k) => k,
            None => continue,
        };
        if db.exists(&key) {
            return RespValue::integer(0);
        }
    }

    // Set all
    for pair in args.chunks(2) {
        let key = match arg_to_string(&pair[0]) {
            Some(k) => k,
            None => continue,
        };
        let value = match arg_to_bytes(&pair[1]) {
            Some(v) => v.to_vec(),
            None => continue,
        };
        db.set(key, Entry::new(RedisValue::String(RedisString::new(value))));
    }

    RespValue::integer(1)
}

pub async fn cmd_append(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("append");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let value = match arg_to_bytes(&args[1]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::String(s) => {
                let new_len = s.append(&value);
                RespValue::integer(new_len as i64)
            }
            _ => wrong_type_error(),
        },
        None => {
            let len = value.len();
            db.set(key, Entry::new(RedisValue::String(RedisString::new(value))));
            RespValue::integer(len as i64)
        }
    }
}

pub async fn cmd_strlen(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("strlen");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => RespValue::integer(s.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

async fn incr_decr(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    delta: i64,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("incr");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::String(s) => match s.incr_by(delta) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(format!("ERR {e}")),
            },
            _ => wrong_type_error(),
        },
        None => {
            db.set(
                key,
                Entry::new(RedisValue::String(RedisString::from_i64(delta))),
            );
            RespValue::integer(delta)
        }
    }
}

pub async fn cmd_incr(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    incr_decr(args, store, client, 1).await
}

pub async fn cmd_decr(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    incr_decr(args, store, client, -1).await
}

pub async fn cmd_incrby(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("incrby");
    }
    let delta = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let key_args = [args[0].clone()];
    incr_decr(&key_args, store, client, delta).await
}

pub async fn cmd_decrby(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("decrby");
    }
    let delta = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let key_args = [args[0].clone()];
    incr_decr(&key_args, store, client, -delta).await
}

pub async fn cmd_incrbyfloat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("incrbyfloat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let delta = match arg_to_f64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not a valid float"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::String(s) => match s.incr_by_float(delta) {
                Ok(n) => RespValue::bulk_string(format!("{n}").into_bytes()),
                Err(e) => RespValue::error(format!("ERR {e}")),
            },
            _ => wrong_type_error(),
        },
        None => {
            db.set(
                key,
                Entry::new(RedisValue::String(RedisString::from_f64(delta))),
            );
            RespValue::bulk_string(format!("{delta}").into_bytes())
        }
    }
}

pub async fn cmd_setnx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("setnx");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let value = match arg_to_bytes(&args[1]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if db.exists(&key) {
        RespValue::integer(0)
    } else {
        db.set(key, Entry::new(RedisValue::String(RedisString::new(value))));
        RespValue::integer(1)
    }
}

pub async fn cmd_setex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("setex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let seconds = match arg_to_i64(&args[1]) {
        Some(n) if n > 0 => n as u64,
        _ => return RespValue::error("ERR invalid expire time in 'setex' command"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let entry = Entry::with_expiry(
        RedisValue::String(RedisString::new(value)),
        now_millis() + seconds * 1000,
    );
    db.set(key, entry);
    RespValue::ok()
}

pub async fn cmd_psetex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("psetex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let millis = match arg_to_i64(&args[1]) {
        Some(n) if n > 0 => n as u64,
        _ => return RespValue::error("ERR invalid expire time in 'psetex' command"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let entry = Entry::with_expiry(
        RedisValue::String(RedisString::new(value)),
        now_millis() + millis,
    );
    db.set(key, entry);
    RespValue::ok()
}

pub async fn cmd_getrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("getrange");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::bulk_string(vec![]),
    };
    let start = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let end = match arg_to_i64(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => {
                let range = s.getrange(start, end);
                RespValue::bulk_string(range.to_vec())
            }
            _ => wrong_type_error(),
        },
        None => RespValue::bulk_string(vec![]),
    }
}

pub async fn cmd_setrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("setrange");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let offset = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::String(s) => {
                let new_len = s.setrange(offset, &value);
                RespValue::integer(new_len as i64)
            }
            _ => wrong_type_error(),
        },
        None => {
            let mut s = RedisString::new(vec![]);
            let new_len = s.setrange(offset, &value);
            db.set(key, Entry::new(RedisValue::String(s)));
            RespValue::integer(new_len as i64)
        }
    }
}

pub async fn cmd_getdel(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("getdel");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let result = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => RespValue::bulk_string(s.as_bytes().to_vec()),
            _ => return wrong_type_error(),
        },
        None => return RespValue::null_bulk_string(),
    };

    db.del(&key);
    result
}

/// GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
pub async fn cmd_getex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("getex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let result = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => RespValue::bulk_string(s.as_bytes().to_vec()),
            _ => return wrong_type_error(),
        },
        None => return RespValue::null_bulk_string(),
    };

    // Parse expiry options
    if args.len() >= 2 {
        let opt = match arg_to_string(&args[1]) {
            Some(s) => s.to_uppercase(),
            None => return result,
        };
        match opt.as_str() {
            "EX" => {
                if let Some(secs) = args.get(2).and_then(arg_to_i64) {
                    if secs > 0 {
                        let expires_at = now_millis() + (secs as u64) * 1000;
                        db.set_expiry(&key, expires_at);
                    }
                }
            }
            "PX" => {
                if let Some(ms) = args.get(2).and_then(arg_to_i64) {
                    if ms > 0 {
                        let expires_at = now_millis() + ms as u64;
                        db.set_expiry(&key, expires_at);
                    }
                }
            }
            "EXAT" => {
                if let Some(ts) = args.get(2).and_then(arg_to_i64) {
                    if ts > 0 {
                        db.set_expiry(&key, (ts as u64) * 1000);
                    }
                }
            }
            "PXAT" => {
                if let Some(ts) = args.get(2).and_then(arg_to_i64) {
                    if ts > 0 {
                        db.set_expiry(&key, ts as u64);
                    }
                }
            }
            "PERSIST" => {
                db.persist(&key);
            }
            _ => {}
        }
    }

    result
}
