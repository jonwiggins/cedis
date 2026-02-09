use crate::command::{arg_to_bytes, arg_to_f64, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::hash::RedisHash;

fn get_or_create_hash<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisHash, RespValue> {
    if !db.exists(key) {
        let hash = RedisHash::new();
        db.set(key.to_string(), Entry::new(RedisValue::Hash(hash)));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Hash(h) => Ok(h),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_hset(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arg_count("hset");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let hash = match get_or_create_hash(db, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    let mut new_fields = 0i64;
    for pair in args[1..].chunks(2) {
        let field = match arg_to_string(&pair[0]) {
            Some(f) => f,
            None => continue,
        };
        let value = match arg_to_bytes(&pair[1]) {
            Some(v) => v.to_vec(),
            None => continue,
        };
        if hash.set(field, value) {
            new_fields += 1;
        }
    }

    RespValue::integer(new_fields)
}

pub async fn cmd_hget(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("hget");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let field = match arg_to_string(&args[1]) {
        Some(f) => f,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => match h.get(&field) {
                Some(v) => RespValue::bulk_string(v.clone()),
                None => RespValue::null_bulk_string(),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_hdel(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("hdel");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Hash(h) => {
                let mut count = 0i64;
                for arg in &args[1..] {
                    if let Some(field) = arg_to_string(arg) {
                        if h.del(&field) {
                            count += 1;
                        }
                    }
                }
                RespValue::integer(count)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_hexists(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("hexists");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let field = match arg_to_string(&args[1]) {
        Some(f) => f,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => RespValue::integer(if h.exists(&field) { 1 } else { 0 }),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_hlen(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("hlen");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => RespValue::integer(h.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_hkeys(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("hkeys");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                let keys: Vec<RespValue> = h
                    .keys()
                    .into_iter()
                    .map(|k| RespValue::bulk_string(k.as_bytes().to_vec()))
                    .collect();
                RespValue::array(keys)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_hvals(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("hvals");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                let vals: Vec<RespValue> = h
                    .values()
                    .into_iter()
                    .map(|v| RespValue::bulk_string(v.clone()))
                    .collect();
                RespValue::array(vals)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_hgetall(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("hgetall");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                let mut result = Vec::new();
                for (field, value) in h.entries() {
                    result.push(RespValue::bulk_string(field.as_bytes().to_vec()));
                    result.push(RespValue::bulk_string(value.clone()));
                }
                RespValue::array(result)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_hmget(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("hmget");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => {
            let nulls: Vec<RespValue> = args[1..].iter().map(|_| RespValue::null_bulk_string()).collect();
            return RespValue::array(nulls);
        }
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                let mut results = Vec::new();
                for arg in &args[1..] {
                    if let Some(field) = arg_to_string(arg) {
                        match h.get(&field) {
                            Some(v) => results.push(RespValue::bulk_string(v.clone())),
                            None => results.push(RespValue::null_bulk_string()),
                        }
                    } else {
                        results.push(RespValue::null_bulk_string());
                    }
                }
                RespValue::array(results)
            }
            _ => wrong_type_error(),
        },
        None => {
            let nulls: Vec<RespValue> = args[1..].iter().map(|_| RespValue::null_bulk_string()).collect();
            RespValue::array(nulls)
        }
    }
}

pub async fn cmd_hincrby(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("hincrby");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let field = match arg_to_string(&args[1]) {
        Some(f) => f,
        None => return RespValue::error("ERR invalid field"),
    };
    let delta = match arg_to_i64(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let hash = match get_or_create_hash(db, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    match hash.incr_by(&field, delta) {
        Ok(n) => RespValue::integer(n),
        Err(e) => RespValue::error(format!("ERR {e}")),
    }
}

pub async fn cmd_hincrbyfloat(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("hincrbyfloat");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let field = match arg_to_string(&args[1]) {
        Some(f) => f,
        None => return RespValue::error("ERR invalid field"),
    };
    let delta = match arg_to_f64(&args[2]) {
        Some(n) => n,
        None => return RespValue::error("ERR value is not a valid float"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let hash = match get_or_create_hash(db, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    match hash.incr_by_float(&field, delta) {
        Ok(n) => RespValue::bulk_string(format!("{n}").into_bytes()),
        Err(e) => RespValue::error(format!("ERR {e}")),
    }
}

pub async fn cmd_hsetnx(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("hsetnx");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let field = match arg_to_string(&args[1]) {
        Some(f) => f,
        None => return RespValue::error("ERR invalid field"),
    };
    let value = match arg_to_bytes(&args[2]) {
        Some(v) => v.to_vec(),
        None => return RespValue::error("ERR invalid value"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let hash = match get_or_create_hash(db, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    RespValue::integer(if hash.setnx(field, value) { 1 } else { 0 })
}

pub async fn cmd_hrandfield(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() || args.len() > 3 {
        return wrong_arg_count("hrandfield");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                if args.len() == 1 {
                    // Return a single random field
                    use rand::seq::IteratorRandom;
                    let mut rng = rand::thread_rng();
                    match h.keys().into_iter().choose(&mut rng) {
                        Some(k) => RespValue::bulk_string(k.as_bytes().to_vec()),
                        None => RespValue::null_bulk_string(),
                    }
                } else {
                    // Count-based
                    let _count = match arg_to_i64(&args[1]) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    let with_values = args.len() == 3;
                    let _ = with_values;
                    // Simplified: return all keys
                    let keys: Vec<RespValue> = h
                        .keys()
                        .into_iter()
                        .map(|k| RespValue::bulk_string(k.as_bytes().to_vec()))
                        .collect();
                    RespValue::array(keys)
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_hscan(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("hscan");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => {
            return RespValue::array(vec![
                RespValue::bulk_string(b"0".to_vec()),
                RespValue::array(vec![]),
            ]);
        }
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                // Simple implementation: return all fields at once
                let mut result = Vec::new();
                for (field, value) in h.entries() {
                    result.push(RespValue::bulk_string(field.as_bytes().to_vec()));
                    result.push(RespValue::bulk_string(value.clone()));
                }
                RespValue::array(vec![
                    RespValue::bulk_string(b"0".to_vec()),
                    RespValue::array(result),
                ])
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![
            RespValue::bulk_string(b"0".to_vec()),
            RespValue::array(vec![]),
        ]),
    }
}
