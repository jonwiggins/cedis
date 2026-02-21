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

    // Parse options: EX, PX, NX, XX, KEEPTTL, GET, IFEQ, IFNE, IFDEQ, IFDNE
    let mut ex: Option<u64> = None;
    let mut px: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get = false;
    let mut ifeq: Option<Vec<u8>> = None;
    let mut ifne: Option<Vec<u8>> = None;
    let mut ifdeq: Option<String> = None;
    let mut ifdne: Option<String> = None;

    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };
        match opt.as_str() {
            "EX" => {
                i += 1;
                let n = match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                // Check for overflow: seconds * 1000 + now must fit in i64
                let n_i128 = n as i128;
                if n_i128 * 1000 + (now_millis() as i128) > i64::MAX as i128 {
                    return RespValue::error("ERR invalid expire time in 'set' command");
                }
                ex = Some(n as u64);
            }
            "PX" => {
                i += 1;
                let n = match arg_to_i64(args.get(i).unwrap_or(&RespValue::null_bulk_string())) {
                    Some(n) if n > 0 => n,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                // Check for overflow: millis + now must fit in i64
                let n_i128 = n as i128;
                if n_i128 + (now_millis() as i128) > i64::MAX as i128 {
                    return RespValue::error("ERR invalid expire time in 'set' command");
                }
                px = Some(n as u64);
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
            "IFEQ" => {
                i += 1;
                ifeq = args.get(i).and_then(arg_to_bytes).map(|v| v.to_vec());
                if ifeq.is_none() {
                    return RespValue::error("ERR syntax error");
                }
            }
            "IFNE" => {
                i += 1;
                ifne = args.get(i).and_then(arg_to_bytes).map(|v| v.to_vec());
                if ifne.is_none() {
                    return RespValue::error("ERR syntax error");
                }
            }
            "IFDEQ" => {
                i += 1;
                ifdeq = args.get(i).and_then(arg_to_string);
                if ifdeq.is_none() {
                    return RespValue::error("ERR syntax error");
                }
            }
            "IFDNE" => {
                i += 1;
                ifdne = args.get(i).and_then(arg_to_string);
                if ifdne.is_none() {
                    return RespValue::error("ERR syntax error");
                }
            }
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

    // IFEQ/IFNE/IFDEQ/IFDNE checks (Redis 8.0+)
    if ifeq.is_some() || ifne.is_some() || ifdeq.is_some() || ifdne.is_some() {
        let current_bytes = match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::String(s) => Some(s.as_bytes().to_vec()),
                _ => return wrong_type_error(),
            },
            None => None,
        };

        let condition_met = if let Some(ref eq_val) = ifeq {
            // IFEQ: key must exist and value must equal
            current_bytes.as_ref().map_or(false, |b| b == eq_val)
        } else if let Some(ref ne_val) = ifne {
            // IFNE: key doesn't exist (OK) or value differs
            current_bytes.as_ref().map_or(true, |b| b != ne_val)
        } else if let Some(ref digest) = ifdeq {
            // IFDEQ: key must exist and digest of value must equal
            if !crate::command::is_valid_digest(digest) {
                return RespValue::error("ERR The digest must be exactly 16 hexadecimal characters");
            }
            current_bytes.as_ref().map_or(false, |b| {
                let hash = crate::command::digest_hash(b);
                hash.eq_ignore_ascii_case(digest)
            })
        } else if let Some(ref digest) = ifdne {
            // IFDNE: key doesn't exist (OK) or digest of value differs
            if !crate::command::is_valid_digest(digest) {
                return RespValue::error("ERR The digest must be exactly 16 hexadecimal characters");
            }
            current_bytes.as_ref().map_or(true, |b| {
                let hash = crate::command::digest_hash(b);
                !hash.eq_ignore_ascii_case(digest)
            })
        } else {
            true
        };

        if !condition_met {
            return old_value.unwrap_or(RespValue::null_bulk_string());
        }
    }

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
    let neg_delta = match delta.checked_neg() {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let key_args = [args[0].clone()];
    incr_decr(&key_args, store, client, neg_delta).await
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
                match s.setrange(offset, &value) {
                    Ok(new_len) => RespValue::integer(new_len as i64),
                    Err(_) => RespValue::error("ERR string exceeds maximum allowed size (512MB)"),
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            // If the key doesn't exist and value is empty, don't create it
            if value.is_empty() {
                return RespValue::integer(0);
            }
            let mut s = RedisString::new(vec![]);
            match s.setrange(offset, &value) {
                Ok(new_len) => {
                    db.set(key, Entry::new(RedisValue::String(s)));
                    RespValue::integer(new_len as i64)
                }
                Err(_) => RespValue::error("ERR string exceeds maximum allowed size (512MB)"),
            }
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
                match args.get(2).and_then(arg_to_i64) {
                    Some(secs) if secs > 0 => {
                        // Check for overflow: seconds * 1000 + now must fit in i64
                        let secs_i128 = secs as i128;
                        if secs_i128 * 1000 + (now_millis() as i128) > i64::MAX as i128 {
                            return RespValue::error("ERR invalid expire time in 'getex' command");
                        }
                        let expires_at = now_millis() + (secs as u64) * 1000;
                        db.set_expiry(&key, expires_at);
                    }
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                }
            }
            "PX" => {
                match args.get(2).and_then(arg_to_i64) {
                    Some(ms) if ms > 0 => {
                        // Check for overflow: millis + now must fit in i64
                        let ms_i128 = ms as i128;
                        if ms_i128 + (now_millis() as i128) > i64::MAX as i128 {
                            return RespValue::error("ERR invalid expire time in 'getex' command");
                        }
                        let expires_at = now_millis() + ms as u64;
                        db.set_expiry(&key, expires_at);
                    }
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                }
            }
            "EXAT" => {
                match args.get(2).and_then(arg_to_i64) {
                    Some(ts) if ts > 0 => {
                        db.set_expiry(&key, (ts as u64) * 1000);
                    }
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                }
            }
            "PXAT" => {
                match args.get(2).and_then(arg_to_i64) {
                    Some(ts) if ts > 0 => {
                        db.set_expiry(&key, ts as u64);
                    }
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                }
            }
            "PERSIST" => {
                db.persist(&key);
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    result
}

/// MSETEX numkeys key value [key value ...] [EX seconds | PX ms | EXAT ts | PXAT ts | KEEPTTL] [NX | XX]
pub async fn cmd_msetex(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("msetex");
    }

    let numkeys_raw = match arg_to_i64(&args[0]) {
        Some(n) if n > 0 => n,
        _ => return RespValue::error("ERR invalid numkeys value"),
    };
    // Overflow protection: reject numkeys > i32::MAX (overflow in 32-bit multiplication)
    if numkeys_raw > i32::MAX as i64 {
        return RespValue::error("ERR invalid numkeys value");
    }
    let numkeys = numkeys_raw as usize;
    let required = 1 + numkeys * 2;

    if args.len() < required {
        return RespValue::error("ERR wrong number of key-value pairs for 'msetex' command");
    }

    let mut pairs = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        let key = match arg_to_string(&args[1 + i * 2]) {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let val = match arg_to_bytes(&args[2 + i * 2]) {
            Some(v) => v,
            None => return RespValue::error("ERR invalid value"),
        };
        pairs.push((key, val));
    }

    let flag_start = 1 + numkeys * 2;
    let mut expire_ms: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut i = flag_start;
    while i < args.len() {
        let flag = match arg_to_string(&args[i]) {
            Some(f) => f.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };
        match flag.as_str() {
            "EX" => {
                if expire_ms.is_some() || keepttl { return RespValue::error("ERR syntax error"); }
                i += 1;
                match args.get(i).and_then(|a| arg_to_i64(a)) {
                    Some(s) if s > 0 => expire_ms = Some(s as u64 * 1000),
                    _ => return RespValue::error("ERR invalid expire time in 'msetex' command"),
                }
            }
            "PX" => {
                if expire_ms.is_some() || keepttl { return RespValue::error("ERR syntax error"); }
                i += 1;
                match args.get(i).and_then(|a| arg_to_i64(a)) {
                    Some(ms) if ms > 0 => expire_ms = Some(ms as u64),
                    _ => return RespValue::error("ERR invalid expire time in 'msetex' command"),
                }
            }
            "EXAT" => {
                if expire_ms.is_some() || keepttl { return RespValue::error("ERR syntax error"); }
                i += 1;
                match args.get(i).and_then(|a| arg_to_i64(a)) {
                    Some(ts) if ts > 0 => expire_ms = Some(ts as u64 * 1000),
                    _ => return RespValue::error("ERR invalid expire time in 'msetex' command"),
                }
            }
            "PXAT" => {
                if expire_ms.is_some() || keepttl { return RespValue::error("ERR syntax error"); }
                i += 1;
                match args.get(i).and_then(|a| arg_to_i64(a)) {
                    Some(ts) if ts > 0 => expire_ms = Some(ts as u64),
                    _ => return RespValue::error("ERR invalid expire time in 'msetex' command"),
                }
            }
            "KEEPTTL" => {
                if expire_ms.is_some() { return RespValue::error("ERR syntax error"); }
                keepttl = true;
            }
            "NX" => {
                if xx { return RespValue::error("ERR syntax error"); }
                nx = true;
            }
            "XX" => {
                if nx { return RespValue::error("ERR syntax error"); }
                xx = true;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let now = now_millis();
    let mut store = store.write().await;
    let db = &mut store.databases[client.db_index];

    if nx {
        for (key, _) in &pairs {
            if db.get(key).is_some() {
                return RespValue::Integer(0);
            }
        }
    }
    if xx {
        for (key, _) in &pairs {
            if db.get(key).is_none() {
                return RespValue::Integer(0);
            }
        }
    }

    for (key, val) in pairs {
        let expires_at = if keepttl {
            db.get_expiry(&key)
        } else {
            expire_ms.map(|ms| now + ms)
        };
        let entry = Entry {
            value: RedisValue::String(RedisString::new(val.to_vec())),
            expires_at,
        };
        db.set(key, entry);
    }

    if nx || xx {
        RespValue::Integer(1)
    } else {
        RespValue::ok()
    }
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
pub async fn cmd_lcs(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("lcs");
    }
    let key1 = match arg_to_string(&args[0]) { Some(k) => k, None => return wrong_arg_count("lcs") };
    let key2 = match arg_to_string(&args[1]) { Some(k) => k, None => return wrong_arg_count("lcs") };

    let mut get_len = false;
    let mut get_idx = false;
    let mut min_match_len: usize = 0;
    let mut with_match_len = false;
    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) { Some(s) => s.to_uppercase(), None => return RespValue::error("ERR syntax error") };
        match opt.as_str() {
            "LEN" => get_len = true,
            "IDX" => get_idx = true,
            "MINMATCHLEN" => {
                i += 1;
                min_match_len = match args.get(i).and_then(|a| arg_to_i64(a)) {
                    Some(n) if n >= 0 => n as usize,
                    _ => return RespValue::error("ERR syntax error"),
                };
            }
            "WITHMATCHLEN" => with_match_len = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = &mut store.databases[client.db_index];
    let a = db.get(&key1).and_then(|e| match &e.value {
        RedisValue::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }).unwrap_or_default();
    let b = db.get(&key2).and_then(|e| match &e.value {
        RedisValue::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }).unwrap_or_default();

    let m = a.len();
    let n = b.len();

    // Build LCS DP table
    let mut dp = vec![vec![0u32; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = std::cmp::max(dp[i - 1][j], dp[i][j - 1]);
            }
        }
    }

    let lcs_len = dp[m][n] as i64;

    if get_len {
        return RespValue::Integer(lcs_len);
    }

    if get_idx {
        // Backtrack to find matching ranges
        let mut matches = Vec::new();
        let mut i = m;
        let mut j = n;
        while i > 0 && j > 0 {
            if a[i - 1] == b[j - 1] {
                let ai_end = i - 1;
                let bj_end = j - 1;
                // Extend match backward
                while i > 1 && j > 1 && a[i - 2] == b[j - 2] {
                    i -= 1;
                    j -= 1;
                }
                let ai_start = i - 1;
                let bj_start = j - 1;
                let match_len = ai_end - ai_start + 1;
                if match_len >= min_match_len {
                    matches.push((ai_start, ai_end, bj_start, bj_end, match_len));
                }
                i -= 1;
                j -= 1;
            } else if dp[i - 1][j] > dp[i][j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }

        let mut result_matches = Vec::new();
        for (ai_start, ai_end, bj_start, bj_end, match_len) in &matches {
            let pair = vec![
                RespValue::array(vec![RespValue::Integer(*ai_start as i64), RespValue::Integer(*ai_end as i64)]),
                RespValue::array(vec![RespValue::Integer(*bj_start as i64), RespValue::Integer(*bj_end as i64)]),
            ];
            if with_match_len {
                result_matches.push(RespValue::array(vec![
                    pair[0].clone(), pair[1].clone(), RespValue::Integer(*match_len as i64),
                ]));
            } else {
                result_matches.push(RespValue::array(pair));
            }
        }

        return RespValue::array(vec![
            RespValue::bulk_string(b"matches".to_vec()),
            RespValue::array(result_matches),
            RespValue::bulk_string(b"len".to_vec()),
            RespValue::Integer(lcs_len),
        ]);
    }

    // Default: return the LCS string
    let mut lcs = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            lcs.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    lcs.reverse();
    RespValue::bulk_string(lcs)
}
