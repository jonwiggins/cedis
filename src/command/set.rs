use crate::command::{arg_to_bytes, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::set::RedisSet;
use std::collections::HashSet;

fn get_or_create_set<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisSet, RespValue> {
    if !db.exists(key) {
        db.set(key.to_string(), Entry::new(RedisValue::Set(RedisSet::new())));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Set(s) => Ok(s),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_sadd(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("sadd");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let set = match get_or_create_set(db, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let mut added = 0i64;
    for arg in &args[1..] {
        if let Some(member) = arg_to_bytes(arg) {
            if set.add(member.to_vec()) {
                added += 1;
            }
        }
    }
    RespValue::integer(added)
}

pub async fn cmd_srem(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("srem");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Set(set) => {
                let mut removed = 0i64;
                for arg in &args[1..] {
                    if let Some(member) = arg_to_bytes(arg) {
                        if set.remove(member) {
                            removed += 1;
                        }
                    }
                }
                RespValue::integer(removed)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_sismember(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("sismember");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let member = match arg_to_bytes(&args[1]) {
        Some(m) => m,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Set(set) => RespValue::integer(if set.contains(member) { 1 } else { 0 }),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_smismember(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("smismember");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => {
            let zeros: Vec<RespValue> = args[1..].iter().map(|_| RespValue::integer(0)).collect();
            return RespValue::array(zeros);
        }
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Set(set) => {
                let results: Vec<RespValue> = args[1..]
                    .iter()
                    .map(|arg| {
                        if let Some(member) = arg_to_bytes(arg) {
                            RespValue::integer(if set.contains(member) { 1 } else { 0 })
                        } else {
                            RespValue::integer(0)
                        }
                    })
                    .collect();
                RespValue::array(results)
            }
            _ => wrong_type_error(),
        },
        None => {
            let zeros: Vec<RespValue> = args[1..].iter().map(|_| RespValue::integer(0)).collect();
            RespValue::array(zeros)
        }
    }
}

pub async fn cmd_smembers(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("smembers");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Set(set) => {
                let members: Vec<RespValue> = set
                    .members()
                    .into_iter()
                    .map(|m| RespValue::bulk_string(m.clone()))
                    .collect();
                RespValue::array(members)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_scard(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("scard");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Set(set) => RespValue::integer(set.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_spop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return wrong_arg_count("spop");
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
            RedisValue::Set(set) => {
                if let Some(count) = count {
                    let mut results = Vec::new();
                    for _ in 0..count {
                        match set.pop() {
                            Some(m) => results.push(RespValue::bulk_string(m)),
                            None => break,
                        }
                    }
                    RespValue::array(results)
                } else {
                    match set.pop() {
                        Some(m) => RespValue::bulk_string(m),
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

pub async fn cmd_srandmember(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return wrong_arg_count("srandmember");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Set(set) => {
                if args.len() == 1 {
                    match set.random_member() {
                        Some(m) => RespValue::bulk_string(m.clone()),
                        None => RespValue::null_bulk_string(),
                    }
                } else {
                    let count = match arg_to_i64(&args[1]) {
                        Some(n) => n,
                        None => return RespValue::error("ERR value is not an integer or out of range"),
                    };
                    let members = set.random_members(count);
                    let resp: Vec<RespValue> = members
                        .into_iter()
                        .map(|m| RespValue::bulk_string(m))
                        .collect();
                    RespValue::array(resp)
                }
            }
            _ => wrong_type_error(),
        },
        None => {
            if args.len() == 1 {
                RespValue::null_bulk_string()
            } else {
                RespValue::array(vec![])
            }
        }
    }
}

// Helper for set operations
fn collect_sets(db: &mut crate::store::Database, keys: &[RespValue]) -> Result<Vec<HashSet<Vec<u8>>>, RespValue> {
    let mut sets = Vec::new();
    for arg in keys {
        if let Some(key) = arg_to_string(arg) {
            match db.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::Set(s) => sets.push(s.iter().cloned().collect()),
                    _ => return Err(wrong_type_error()),
                },
                None => sets.push(HashSet::new()),
            }
        }
    }
    Ok(sets)
}

pub async fn cmd_sunion(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("sunion");
    }
    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let sets = match collect_sets(db, args) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for set in sets {
        result.extend(set);
    }

    let resp: Vec<RespValue> = result.into_iter().map(|m| RespValue::bulk_string(m)).collect();
    RespValue::array(resp)
}

pub async fn cmd_sinter(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("sinter");
    }
    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let sets = match collect_sets(db, args) {
        Ok(s) => s,
        Err(e) => return e,
    };

    if sets.is_empty() {
        return RespValue::array(vec![]);
    }

    let mut result = sets[0].clone();
    for set in &sets[1..] {
        result = result.intersection(set).cloned().collect();
    }

    let resp: Vec<RespValue> = result.into_iter().map(|m| RespValue::bulk_string(m)).collect();
    RespValue::array(resp)
}

pub async fn cmd_sdiff(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("sdiff");
    }
    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let sets = match collect_sets(db, args) {
        Ok(s) => s,
        Err(e) => return e,
    };

    if sets.is_empty() {
        return RespValue::array(vec![]);
    }

    let mut result = sets[0].clone();
    for set in &sets[1..] {
        result = result.difference(set).cloned().collect();
    }

    let resp: Vec<RespValue> = result.into_iter().map(|m| RespValue::bulk_string(m)).collect();
    RespValue::array(resp)
}

async fn set_store_op(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    op: &str,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count(op);
    }
    let dest = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store_lock = store.write().await;
    let db = store_lock.db(client.db_index);

    let sets = match collect_sets(db, &args[1..]) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let result = if sets.is_empty() {
        HashSet::new()
    } else {
        let mut result = sets[0].clone();
        for set in &sets[1..] {
            result = match op {
                "sunionstore" => result.union(set).cloned().collect(),
                "sinterstore" => result.intersection(set).cloned().collect(),
                "sdiffstore" => result.difference(set).cloned().collect(),
                _ => unreachable!(),
            };
        }
        result
    };

    let len = result.len() as i64;
    db.set(dest, Entry::new(RedisValue::Set(RedisSet::from_set(result))));
    RespValue::integer(len)
}

pub async fn cmd_sunionstore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    set_store_op(args, store, client, "sunionstore").await
}

pub async fn cmd_sinterstore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    set_store_op(args, store, client, "sinterstore").await
}

pub async fn cmd_sdiffstore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    set_store_op(args, store, client, "sdiffstore").await
}

pub async fn cmd_smove(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("smove");
    }
    let src = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let dst = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let member = match arg_to_bytes(&args[2]) {
        Some(m) => m.to_vec(),
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Remove from source
    let removed = match db.get_mut(&src) {
        Some(entry) => match &mut entry.value {
            RedisValue::Set(set) => set.remove(&member),
            _ => return wrong_type_error(),
        },
        None => return RespValue::integer(0),
    };

    if !removed {
        return RespValue::integer(0);
    }

    // Add to destination
    let dest_set = match get_or_create_set(db, &dst) {
        Ok(s) => s,
        Err(e) => return e,
    };
    dest_set.add(member);

    RespValue::integer(1)
}

pub async fn cmd_sscan(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("sscan");
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
            RedisValue::Set(set) => {
                let members: Vec<RespValue> = set
                    .members()
                    .into_iter()
                    .map(|m| RespValue::bulk_string(m.clone()))
                    .collect();
                RespValue::array(vec![
                    RespValue::bulk_string(b"0".to_vec()),
                    RespValue::array(members),
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

pub async fn cmd_sintercard(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("sintercard");
    }
    let numkeys = match arg_to_i64(&args[0]) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::error("ERR Number of keys can't be non-positive value"),
    };

    if args.len() < 1 + numkeys {
        return wrong_arg_count("sintercard");
    }

    let mut limit = 0usize;
    let key_end = 1 + numkeys;
    if args.len() > key_end {
        if let Some(s) = arg_to_string(&args[key_end]) {
            if s.to_uppercase() == "LIMIT" && args.len() > key_end + 1 {
                limit = arg_to_i64(&args[key_end + 1]).unwrap_or(0) as usize;
            }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let sets = match collect_sets(db, &args[1..1 + numkeys]) {
        Ok(s) => s,
        Err(e) => return e,
    };

    if sets.is_empty() {
        return RespValue::integer(0);
    }

    let mut result = sets[0].clone();
    for set in &sets[1..] {
        result = result.intersection(set).cloned().collect();
    }

    let count = if limit > 0 {
        result.len().min(limit)
    } else {
        result.len()
    };

    RespValue::integer(count as i64)
}
