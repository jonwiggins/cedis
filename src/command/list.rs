use crate::command::{arg_to_bytes, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::list::RedisList;
use std::time::Duration;

/// Parse a blocking timeout argument (supports both integer and float values).
/// Returns a Duration: 0 means block indefinitely (1 year), otherwise the specified seconds.
fn parse_blocking_timeout(arg: &RespValue) -> Result<Duration, RespValue> {
    let s = arg
        .to_string_lossy()
        .ok_or_else(|| RespValue::error("ERR timeout is not a float or out of range"))?;
    let timeout: f64 = s
        .parse()
        .map_err(|_| RespValue::error("ERR timeout is not a float or out of range"))?;
    if timeout < 0.0 {
        return Err(RespValue::error(
            "ERR timeout is not a float or out of range",
        ));
    }
    if timeout == 0.0 {
        Ok(Duration::from_secs(365 * 24 * 3600)) // Block indefinitely per Redis spec
    } else {
        Ok(Duration::from_secs_f64(timeout))
    }
}

fn get_or_create_list<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisList, RespValue> {
    if !db.exists(key) {
        let list = RedisList::new();
        db.set(key.to_string(), Entry::new(RedisValue::List(list)));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(l) => Ok(l),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_lpushx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("lpushx");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Only push if the key already exists and is a list
    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                for arg in &args[1..] {
                    if let Some(value) = arg_to_bytes(arg) {
                        list.lpush(value.to_vec());
                    }
                }
                let len = list.len() as i64;
                drop(store);
                let mut watcher = key_watcher.write().await;
                watcher.notify(&key);
                RespValue::integer(len)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_rpushx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("rpushx");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                for arg in &args[1..] {
                    if let Some(value) = arg_to_bytes(arg) {
                        list.rpush(value.to_vec());
                    }
                }
                let len = list.len() as i64;
                drop(store);
                let mut watcher = key_watcher.write().await;
                watcher.notify(&key);
                RespValue::integer(len)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_lpush(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("lpush");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let list = match get_or_create_list(db, &key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    for arg in &args[1..] {
        if let Some(value) = arg_to_bytes(arg) {
            list.lpush(value.to_vec());
        }
    }

    let len = list.len() as i64;
    drop(store);

    // Notify any clients blocked on this key
    let mut watcher = key_watcher.write().await;
    watcher.notify(&key);

    RespValue::integer(len)
}

pub async fn cmd_rpush(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("rpush");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let list = match get_or_create_list(db, &key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    for arg in &args[1..] {
        if let Some(value) = arg_to_bytes(arg) {
            list.rpush(value.to_vec());
        }
    }

    let len = list.len() as i64;
    drop(store);

    // Notify any clients blocked on this key
    let mut watcher = key_watcher.write().await;
    watcher.notify(&key);

    RespValue::integer(len)
}

pub async fn cmd_lpop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return wrong_arg_count("lpop");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let count = if args.len() == 2 {
        match arg_to_i64(&args[1]) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                if let Some(count) = count {
                    let mut results = Vec::new();
                    for _ in 0..count {
                        match list.lpop() {
                            Some(v) => results.push(RespValue::bulk_string(v)),
                            None => break,
                        }
                    }
                    if results.is_empty() {
                        RespValue::array(vec![])
                    } else {
                        RespValue::array(results)
                    }
                } else {
                    match list.lpop() {
                        Some(v) => RespValue::bulk_string(v),
                        None => RespValue::null_bulk_string(),
                    }
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            if count.is_some() {
                RespValue::null_array()
            } else {
                RespValue::null_bulk_string()
            }
        }
    }
}

pub async fn cmd_rpop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return wrong_arg_count("rpop");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let count = if args.len() == 2 {
        match arg_to_i64(&args[1]) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                if let Some(count) = count {
                    let mut results = Vec::new();
                    for _ in 0..count {
                        match list.rpop() {
                            Some(v) => results.push(RespValue::bulk_string(v)),
                            None => break,
                        }
                    }
                    if results.is_empty() {
                        RespValue::array(vec![])
                    } else {
                        RespValue::array(results)
                    }
                } else {
                    match list.rpop() {
                        Some(v) => RespValue::bulk_string(v),
                        None => RespValue::null_bulk_string(),
                    }
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            if count.is_some() {
                RespValue::null_array()
            } else {
                RespValue::null_bulk_string()
            }
        }
    }
}

pub async fn cmd_llen(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("llen");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::List(list) => RespValue::integer(list.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_lrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("lrange");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let start = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop = match arg_to_i64(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::List(list) => {
                let items = list.lrange(start, stop);
                let resp: Vec<RespValue> = items
                    .into_iter()
                    .map(|v| RespValue::bulk_string(v.clone()))
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_lindex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("lindex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let index = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::List(list) => match list.lindex(index) {
                Some(v) => RespValue::bulk_string(v.clone()),
                None => RespValue::null_bulk_string(),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_lset(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("lset");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR no such key"),
    };
    let index = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                if list.lset(index, value) {
                    RespValue::ok()
                } else {
                    RespValue::error("ERR index out of range")
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR no such key"),
    }
}

pub async fn cmd_linsert(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 4 {
        return wrong_arg_count("linsert");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let position = match arg_to_string(&args[1]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    let pivot = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid pivot"),
    };
    let value = match arg_to_bytes(&args[3]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                let result = match position.as_str() {
                    "BEFORE" => list.linsert_before(&pivot, value),
                    "AFTER" => list.linsert_after(&pivot, value),
                    _ => return RespValue::error("ERR syntax error"),
                };
                match result {
                    Some(len) => RespValue::integer(len as i64),
                    None => RespValue::integer(-1),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_lrem(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("lrem");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let count = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => RespValue::integer(list.lrem(count, &value)),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_ltrim(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("ltrim");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::ok(),
    };
    let start = match arg_to_i64(&args[1]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop = match arg_to_i64(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => {
                list.ltrim(start, stop);
                RespValue::ok()
            }
            _ => wrong_type_error(),
        },
        None => RespValue::ok(),
    }
}

pub async fn cmd_rpoplpush(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("rpoplpush");
    }
    let src = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let dst = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Pop from source
    let value = match db.get_mut(&src) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => list.rpop(),
            _ => return wrong_type_error(),
        },
        None => return RespValue::null_bulk_string(),
    };

    let value = match value {
        Some(v) => v,
        None => return RespValue::null_bulk_string(),
    };

    // Push to destination
    let list = match get_or_create_list(db, &dst) {
        Ok(l) => l,
        Err(e) => return e,
    };
    list.lpush(value.clone());

    // Clean up empty source
    if let Some(entry) = db.get(&src)
        && let RedisValue::List(list) = &entry.value
        && list.is_empty()
    {
        db.del(&src);
    }

    RespValue::bulk_string(value)
}

pub async fn cmd_lmove(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 4 {
        return wrong_arg_count("lmove");
    }
    let src = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let dst = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let wherefrom = match arg_to_string(&args[2]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    let whereto = match arg_to_string(&args[3]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let value = match db.get_mut(&src) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => match wherefrom.as_str() {
                "LEFT" => list.lpop(),
                "RIGHT" => list.rpop(),
                _ => return RespValue::error("ERR syntax error"),
            },
            _ => return wrong_type_error(),
        },
        None => return RespValue::null_bulk_string(),
    };

    let value = match value {
        Some(v) => v,
        None => return RespValue::null_bulk_string(),
    };

    let list = match get_or_create_list(db, &dst) {
        Ok(l) => l,
        Err(e) => return e,
    };

    match whereto.as_str() {
        "LEFT" => list.lpush(value.clone()),
        "RIGHT" => list.rpush(value.clone()),
        _ => return RespValue::error("ERR syntax error"),
    }

    RespValue::bulk_string(value)
}

pub async fn cmd_lmpop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 3 {
        return wrong_arg_count("lmpop");
    }
    let numkeys = match arg_to_i64(&args[0]) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::error("ERR numkeys must be positive"),
    };
    if args.len() < 1 + numkeys + 1 {
        return wrong_arg_count("lmpop");
    }

    let keys: Vec<String> = args[1..1 + numkeys]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let direction_idx = 1 + numkeys;
    let direction = match arg_to_string(&args[direction_idx]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    if direction != "LEFT" && direction != "RIGHT" {
        return RespValue::error("ERR syntax error");
    }

    let mut count = 1usize;
    let mut i = direction_idx + 1;
    while i < args.len() {
        if let Some(opt) = arg_to_string(&args[i])
            && opt.to_uppercase() == "COUNT"
            && i + 1 < args.len()
        {
            count = arg_to_i64(&args[i + 1]).unwrap_or(1).max(1) as usize;
            i += 1;
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    for key in &keys {
        let list_len = match db.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::List(list) => list.len(),
                _ => continue,
            },
            None => continue,
        };
        if list_len == 0 {
            continue;
        }

        let entry = db.get_mut(key).unwrap();
        let list = match &mut entry.value {
            RedisValue::List(l) => l,
            _ => continue,
        };

        let mut results = Vec::new();
        for _ in 0..count {
            let val = if direction == "LEFT" {
                list.lpop()
            } else {
                list.rpop()
            };
            match val {
                Some(v) => results.push(RespValue::bulk_string(v)),
                None => break,
            }
        }

        if !results.is_empty() {
            // Check if list became empty and clean up
            let should_del = list.is_empty();
            let key_clone = key.clone();
            if should_del {
                db.del(&key_clone);
            }

            return RespValue::array(vec![
                RespValue::bulk_string(key_clone.into_bytes()),
                RespValue::array(results),
            ]);
        }
    }

    RespValue::null_array()
}

pub async fn cmd_blpop(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    // BLPOP key [key ...] timeout
    if args.len() < 2 {
        return wrong_arg_count("blpop");
    }

    let keys: Vec<String> = args[..args.len() - 1]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let timeout_dur = match parse_blocking_timeout(&args[args.len() - 1]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    // Try immediate pop first
    {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_lpop_from_keys(db, &keys) {
            return result;
        }
    }

    // Register a single shared Notify for all keys
    let notify = {
        let mut watcher = key_watcher.write().await;
        watcher.register_many(&keys)
    };

    // Wait for notification or timeout
    let notified = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(timeout_dur) => false,
    };

    // Clean up watchers
    {
        let mut watcher = key_watcher.write().await;
        watcher.unregister_many(&keys, &notify);
    }

    if !notified {
        return RespValue::null_array();
    }

    // Try to pop after being notified
    let mut store = store.write().await;
    let db = store.db(client.db_index);
    try_lpop_from_keys(db, &keys).unwrap_or_else(RespValue::null_array)
}

pub async fn cmd_brpop(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    // BRPOP key [key ...] timeout
    if args.len() < 2 {
        return wrong_arg_count("brpop");
    }

    let keys: Vec<String> = args[..args.len() - 1]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let timeout_dur = match parse_blocking_timeout(&args[args.len() - 1]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    // Try immediate pop first
    {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_rpop_from_keys(db, &keys) {
            return result;
        }
    }

    // Register a single shared Notify for all keys
    let notify = {
        let mut watcher = key_watcher.write().await;
        watcher.register_many(&keys)
    };

    let notified = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(timeout_dur) => false,
    };

    {
        let mut watcher = key_watcher.write().await;
        watcher.unregister_many(&keys, &notify);
    }

    if !notified {
        return RespValue::null_array();
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    try_rpop_from_keys(db, &keys).unwrap_or_else(RespValue::null_array)
}

/// Try to LPOP from the first non-empty list key. Returns None if all empty.
fn try_lpop_from_keys(db: &mut crate::store::Database, keys: &[String]) -> Option<RespValue> {
    for key in keys {
        match db.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) if !list.is_empty() => {
                    let val = list.lpop().unwrap();
                    let should_del = list.is_empty();
                    let key_clone = key.clone();
                    let key_resp = RespValue::bulk_string(key_clone.as_bytes().to_vec());
                    let val_resp = RespValue::bulk_string(val);
                    if should_del {
                        db.del(&key_clone);
                    }
                    return Some(RespValue::array(vec![key_resp, val_resp]));
                }
                _ => continue,
            },
            None => continue,
        }
    }
    None
}

/// Try to RPOP from the first non-empty list key. Returns None if all empty.
fn try_rpop_from_keys(db: &mut crate::store::Database, keys: &[String]) -> Option<RespValue> {
    for key in keys {
        match db.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) if !list.is_empty() => {
                    let val = list.rpop().unwrap();
                    let should_del = list.is_empty();
                    let key_clone = key.clone();
                    let key_resp = RespValue::bulk_string(key_clone.as_bytes().to_vec());
                    let val_resp = RespValue::bulk_string(val);
                    if should_del {
                        db.del(&key_clone);
                    }
                    return Some(RespValue::array(vec![key_resp, val_resp]));
                }
                _ => continue,
            },
            None => continue,
        }
    }
    None
}

pub async fn cmd_lpos(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    // LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
    if args.len() < 2 {
        return wrong_arg_count("lpos");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let element = match arg_to_bytes(&args[1]) {
        Some(v) => v.to_vec(),
        None => return RespValue::null_bulk_string(),
    };

    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: usize = 0;

    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR syntax error"),
        };
        match opt.as_str() {
            "RANK" => {
                i += 1;
                rank = match args.get(i).and_then(arg_to_i64) {
                    Some(r) if r != 0 => r,
                    _ => {
                        return RespValue::error(
                            "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                        );
                    }
                };
            }
            "COUNT" => {
                i += 1;
                count = match args.get(i).and_then(arg_to_i64) {
                    Some(c) if c >= 0 => Some(c),
                    _ => return RespValue::error("ERR COUNT can't be negative"),
                };
            }
            "MAXLEN" => {
                i += 1;
                maxlen = match args.get(i).and_then(arg_to_i64) {
                    Some(m) if m >= 0 => m as usize,
                    _ => return RespValue::error("ERR MAXLEN can't be negative"),
                };
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::List(list) => {
                let positions = list.lpos(&element, rank, count, maxlen);
                if count.is_some() {
                    // COUNT specified: always return array
                    let resp: Vec<RespValue> = positions
                        .iter()
                        .map(|&p| RespValue::integer(p as i64))
                        .collect();
                    RespValue::array(resp)
                } else {
                    // No COUNT: return single value or null
                    match positions.first() {
                        Some(&pos) => RespValue::integer(pos as i64),
                        None => RespValue::null_bulk_string(),
                    }
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            if count.is_some() {
                RespValue::array(vec![])
            } else {
                RespValue::null_bulk_string()
            }
        }
    }
}

pub async fn cmd_blmove(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    // BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    if args.len() != 5 {
        return wrong_arg_count("blmove");
    }
    let src = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let dst = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let wherefrom = match arg_to_string(&args[2]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    let whereto = match arg_to_string(&args[3]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    if !matches!(wherefrom.as_str(), "LEFT" | "RIGHT")
        || !matches!(whereto.as_str(), "LEFT" | "RIGHT")
    {
        return RespValue::error("ERR syntax error");
    }

    let timeout_dur = match parse_blocking_timeout(&args[4]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    // Try immediate move first
    {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_lmove(db, &src, &dst, &wherefrom, &whereto) {
            return result;
        }
    }

    let keys = vec![src.clone()];
    let notify = {
        let mut watcher = key_watcher.write().await;
        watcher.register_many(&keys)
    };

    let notified = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(timeout_dur) => false,
    };

    {
        let mut watcher = key_watcher.write().await;
        watcher.unregister_many(&keys, &notify);
    }

    if !notified {
        return RespValue::null_bulk_string();
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    try_lmove(db, &src, &dst, &wherefrom, &whereto).unwrap_or_else(RespValue::null_bulk_string)
}

pub async fn cmd_blmpop(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 4 {
        return wrong_arg_count("blmpop");
    }

    let timeout_dur = match parse_blocking_timeout(&args[0]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let numkeys = match arg_to_i64(&args[1]) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::error("ERR numkeys must be positive"),
    };

    if args.len() < 2 + numkeys + 1 {
        return wrong_arg_count("blmpop");
    }

    let keys: Vec<String> = args[2..2 + numkeys]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let direction_idx = 2 + numkeys;
    let direction = match arg_to_string(&args[direction_idx]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    if direction != "LEFT" && direction != "RIGHT" {
        return RespValue::error("ERR syntax error");
    }

    let mut count = 1usize;
    let mut i = direction_idx + 1;
    while i < args.len() {
        if let Some(opt) = arg_to_string(&args[i])
            && opt.to_uppercase() == "COUNT"
            && i + 1 < args.len()
        {
            count = arg_to_i64(&args[i + 1]).unwrap_or(1).max(1) as usize;
            i += 1;
        }
        i += 1;
    }

    // Try immediate pop first
    {
        let mut store_w = store.write().await;
        let db = store_w.db(client.db_index);
        if let Some(result) = try_lmpop(db, &keys, &direction, count) {
            return result;
        }
    }

    let notify = {
        let mut watcher = key_watcher.write().await;
        watcher.register_many(&keys)
    };

    let notified = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(timeout_dur) => false,
    };

    {
        let mut watcher = key_watcher.write().await;
        watcher.unregister_many(&keys, &notify);
    }

    if !notified {
        return RespValue::null_array();
    }

    let mut store_w = store.write().await;
    let db = store_w.db(client.db_index);
    try_lmpop(db, &keys, &direction, count).unwrap_or_else(RespValue::null_array)
}

/// Try to LMOVE from src to dst. Returns None if source is empty/missing.
fn try_lmove(
    db: &mut crate::store::Database,
    src: &str,
    dst: &str,
    wherefrom: &str,
    whereto: &str,
) -> Option<RespValue> {
    let value = match db.get_mut(src) {
        Some(entry) => match &mut entry.value {
            RedisValue::List(list) => match wherefrom {
                "LEFT" => list.lpop(),
                "RIGHT" => list.rpop(),
                _ => return Some(RespValue::error("ERR syntax error")),
            },
            _ => return Some(wrong_type_error()),
        },
        None => return None,
    };

    let value = value?;

    // Clean up empty source before creating destination (handles src == dst)
    let src_empty = match db.get(src) {
        Some(entry) => match &entry.value {
            RedisValue::List(list) => list.is_empty(),
            _ => false,
        },
        None => false,
    };
    if src_empty {
        db.del(src);
    }

    let list = match get_or_create_list(db, dst) {
        Ok(l) => l,
        Err(e) => return Some(e),
    };

    match whereto {
        "LEFT" => list.lpush(value.clone()),
        "RIGHT" => list.rpush(value.clone()),
        _ => return Some(RespValue::error("ERR syntax error")),
    }

    Some(RespValue::bulk_string(value))
}

/// Try to LMPOP from the first non-empty list key. Returns None if all empty.
fn try_lmpop(
    db: &mut crate::store::Database,
    keys: &[String],
    direction: &str,
    count: usize,
) -> Option<RespValue> {
    for key in keys {
        let list_len = match db.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::List(list) => list.len(),
                _ => continue,
            },
            None => continue,
        };
        if list_len == 0 {
            continue;
        }

        let entry = db.get_mut(key).unwrap();
        let list = match &mut entry.value {
            RedisValue::List(l) => l,
            _ => continue,
        };

        let mut results = Vec::new();
        for _ in 0..count {
            let val = if direction == "LEFT" {
                list.lpop()
            } else {
                list.rpop()
            };
            match val {
                Some(v) => results.push(RespValue::bulk_string(v)),
                None => break,
            }
        }

        if !results.is_empty() {
            let should_del = list.is_empty();
            let key_clone = key.clone();
            if should_del {
                db.del(&key_clone);
            }
            return Some(RespValue::array(vec![
                RespValue::bulk_string(key_clone.into_bytes()),
                RespValue::array(results),
            ]));
        }
    }
    None
}
