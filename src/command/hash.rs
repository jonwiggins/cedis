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

pub async fn cmd_hmset(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return wrong_arg_count("hmset");
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

    for pair in args[1..].chunks(2) {
        let field = match arg_to_string(&pair[0]) {
            Some(f) => f,
            None => continue,
        };
        let value = match arg_to_bytes(&pair[1]) {
            Some(v) => v.to_vec(),
            None => continue,
        };
        hash.set(field, value);
    }

    RespValue::ok()
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

    let is_hash = matches!(db.get(&key), Some(entry) if matches!(&entry.value, RedisValue::Hash(_)));
    if !is_hash {
        return match db.get(&key) {
            Some(_) => wrong_type_error(),
            None => RespValue::integer(0),
        };
    }

    let mut count = 0i64;
    if let Some(entry) = db.get_mut(&key) {
        if let RedisValue::Hash(h) = &mut entry.value {
            for arg in &args[1..] {
                if let Some(field) = arg_to_string(arg) {
                    if h.del(&field) {
                        count += 1;
                    }
                }
            }
        }
    }

    // Auto-delete key when hash becomes empty
    if let Some(entry) = db.get(&key) {
        if let RedisValue::Hash(h) = &entry.value {
            if h.len() == 0 {
                db.del(&key);
            }
        }
    }

    RespValue::integer(count)
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

    if delta.is_nan() || delta.is_infinite() {
        return RespValue::error("ERR value is NaN or Infinity");
    }

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

pub async fn cmd_hstrlen(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("hstrlen");
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
            RedisValue::Hash(h) => match h.get(&field) {
                Some(v) => RespValue::integer(v.len() as i64),
                None => RespValue::integer(0),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_hgetdel(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // HGETDEL key FIELDS numfields field [field ...]
    if args.len() < 4 {
        return wrong_arg_count("hgetdel");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    // args[1] should be "FIELDS"
    match arg_to_string(&args[1]).map(|s| s.to_uppercase()) {
        Some(ref s) if s == "FIELDS" => {}
        _ => return RespValue::error("ERR Mandatory argument FIELDS is missing or not at the right position"),
    }
    let numfields = match arg_to_i64(&args[2]) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::error("ERR Number of fields must be a positive integer"),
    };
    if args.len() != 3 + numfields {
        return RespValue::error("ERR The `numfields` parameter must match the number of arguments");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let is_hash = matches!(db.get(&key), Some(entry) if matches!(&entry.value, RedisValue::Hash(_)));
    if !is_hash {
        return match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::Hash(_) => unreachable!(),
                _ => wrong_type_error(),
            },
            None => {
                let results: Vec<RespValue> = args[3..].iter().map(|_| RespValue::null_bulk_string()).collect();
                RespValue::array(results)
            }
        };
    }

    let mut results = Vec::new();
    if let Some(entry) = db.get_mut(&key) {
        if let RedisValue::Hash(h) = &mut entry.value {
            for arg in &args[3..] {
                if let Some(field) = arg_to_string(arg) {
                    match h.get(&field) {
                        Some(v) => {
                            let val = v.clone();
                            h.del(&field);
                            results.push(RespValue::bulk_string(val));
                        }
                        None => results.push(RespValue::null_bulk_string()),
                    }
                } else {
                    results.push(RespValue::null_bulk_string());
                }
            }
        }
    }

    // Auto-delete key when hash becomes empty
    if let Some(entry) = db.get(&key) {
        if let RedisValue::Hash(h) = &entry.value {
            if h.len() == 0 {
                db.del(&key);
            }
        }
    }

    RespValue::array(results)
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
                    let count = match arg_to_i64(&args[1]) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    let with_values = args.len() == 3;
                    let allow_dups = count < 0;
                    let abs_count = count.unsigned_abs() as usize;

                    if abs_count == 0 {
                        return RespValue::array(vec![]);
                    }

                    // Protect against OOM with extreme negative counts
                    // (matches Redis threshold: abs(count) >= LONG_MAX/2)
                    if allow_dups && abs_count >= (i64::MAX as usize) / 2 {
                        return RespValue::error("ERR value is out of range");
                    }

                    use rand::seq::IteratorRandom;
                    let mut rng = rand::thread_rng();
                    let all_entries: Vec<(String, Vec<u8>)> = h.entries().into_iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                    if all_entries.is_empty() {
                        return RespValue::array(vec![]);
                    }

                    let mut result = Vec::new();
                    if allow_dups {
                        // Negative count: allow duplicates, return exactly abs_count elements
                        for _ in 0..abs_count {
                            let idx = rand::Rng::gen_range(&mut rng, 0..all_entries.len());
                            let (k, v) = &all_entries[idx];
                            result.push(RespValue::bulk_string(k.as_bytes().to_vec()));
                            if with_values {
                                result.push(RespValue::bulk_string(v.clone()));
                            }
                        }
                    } else {
                        // Positive count: unique elements, at most count
                        let take = abs_count.min(all_entries.len());
                        let indices: Vec<usize> = (0..all_entries.len()).choose_multiple(&mut rng, take);
                        for idx in indices {
                            let (k, v) = &all_entries[idx];
                            result.push(RespValue::bulk_string(k.as_bytes().to_vec()));
                            if with_values {
                                result.push(RespValue::bulk_string(v.clone()));
                            }
                        }
                    }
                    RespValue::array(result)
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            if args.len() >= 2 {
                // Count variant with non-existing key returns empty array
                RespValue::array(vec![])
            } else {
                RespValue::null_bulk_string()
            }
        }
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

    // Parse optional MATCH, COUNT, NOVALUES
    let mut pattern: Option<String> = None;
    let mut novalues = false;
    let mut i = 2;
    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => { i += 1; continue; }
        };
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = args.get(i).and_then(|a| arg_to_string(a));
            }
            "COUNT" => { i += 1; } // skip count value
            "NOVALUES" => { novalues = true; }
            _ => {}
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Hash(h) => {
                let mut result = Vec::new();
                for (field, value) in h.entries() {
                    if let Some(ref pat) = pattern {
                        if !crate::glob::glob_match(pat, field) {
                            continue;
                        }
                    }
                    result.push(RespValue::bulk_string(field.as_bytes().to_vec()));
                    if !novalues {
                        result.push(RespValue::bulk_string(value.clone()));
                    }
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
