use crate::command::{arg_to_bytes, arg_to_f64, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::sorted_set::RedisSortedSet;

fn get_or_create_zset<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisSortedSet, RespValue> {
    if !db.exists(key) {
        db.set(key.to_string(), Entry::new(RedisValue::SortedSet(RedisSortedSet::new())));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::SortedSet(z) => Ok(z),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_zadd(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zadd");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Parse flags
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;
    let mut i = 1;

    while i < args.len() {
        let opt = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => break,
        };
        match opt.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "GT" => { gt = true; i += 1; }
            "LT" => { lt = true; i += 1; }
            "CH" => { ch = true; i += 1; }
            "INCR" => { incr = true; i += 1; }
            _ => break,
        }
    }

    // Remaining args are score-member pairs
    let pairs = &args[i..];
    if pairs.is_empty() || pairs.len() % 2 != 0 {
        return wrong_arg_count("zadd");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if incr {
        // INCR mode: single score-member pair, return the new score
        if pairs.len() != 2 {
            return RespValue::error("ERR INCR option supports a single increment-element pair");
        }
        let score = match arg_to_f64(&pairs[0]) {
            Some(s) if !s.is_nan() => s,
            _ => return RespValue::error("ERR value is not a valid float"),
        };
        let member = match arg_to_bytes(&pairs[1]) {
            Some(m) => m.to_vec(),
            None => return RespValue::error("ERR invalid member"),
        };

        let zset = match get_or_create_zset(db, &key) {
            Ok(z) => z,
            Err(e) => return e,
        };

        let exists = zset.contains(&member);
        if nx && exists { return RespValue::null_bulk_string(); }
        if xx && !exists { return RespValue::null_bulk_string(); }

        let new_score = zset.incr_by(member, score);
        RespValue::bulk_string(format!("{new_score}").into_bytes())
    } else {
        let zset = match get_or_create_zset(db, &key) {
            Ok(z) => z,
            Err(e) => return e,
        };

        let mut added = 0i64;
        let mut changed = 0i64;

        for pair in pairs.chunks(2) {
            let score = match arg_to_f64(&pair[0]) {
                Some(s) if !s.is_nan() => s,
                _ => return RespValue::error("ERR value is not a valid float"),
            };
            let member = match arg_to_bytes(&pair[1]) {
                Some(m) => m.to_vec(),
                None => continue,
            };

            let exists = zset.contains(&member);
            let old_score = zset.score(&member);

            if nx && exists { continue; }
            if xx && !exists { continue; }

            // GT/LT checks
            if let Some(old) = old_score {
                if gt && score <= old { continue; }
                if lt && score >= old { continue; }
            }

            let is_new = zset.add(member, score);
            if is_new {
                added += 1;
            } else if old_score != Some(score) {
                changed += 1;
            }
        }

        RespValue::integer(if ch { added + changed } else { added })
    }
}

pub async fn cmd_zrem(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("zrem");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::SortedSet(zset) => {
                let mut removed = 0i64;
                for arg in &args[1..] {
                    if let Some(member) = arg_to_bytes(arg) {
                        if zset.remove(member) {
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

pub async fn cmd_zscore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("zscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let member = match arg_to_bytes(&args[1]) {
        Some(m) => m,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => match zset.score(member) {
                Some(s) => RespValue::bulk_string(format!("{s}").into_bytes()),
                None => RespValue::null_bulk_string(),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_zrank(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("zrank");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let member = match arg_to_bytes(&args[1]) {
        Some(m) => m,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => match zset.rank(member) {
                Some(r) => RespValue::integer(r as i64),
                None => RespValue::null_bulk_string(),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_zrevrank(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("zrevrank");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };
    let member = match arg_to_bytes(&args[1]) {
        Some(m) => m,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => match zset.rev_rank(member) {
                Some(r) => RespValue::integer(r as i64),
                None => RespValue::null_bulk_string(),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_zcard(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("zcard");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => RespValue::integer(zset.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

pub async fn cmd_zcount(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zcount");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let min = match parse_score_bound(&args[1], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let max = match parse_score_bound(&args[2], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => RespValue::integer(zset.count(min, max) as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

fn format_range_result(items: Vec<(&[u8], f64)>, withscores: bool) -> RespValue {
    let mut result = Vec::new();
    for (member, score) in items {
        result.push(RespValue::bulk_string(member.to_vec()));
        if withscores {
            result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
        }
    }
    RespValue::array(result)
}

pub async fn cmd_zrange(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrange");
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

    let withscores = args.len() > 3
        && arg_to_string(&args[3])
            .map(|s| s.to_uppercase() == "WITHSCORES")
            .unwrap_or(false);

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let items = zset.range(start, stop);
                format_range_result(items, withscores)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrevrange(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrevrange");
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

    let withscores = args.len() > 3
        && arg_to_string(&args[3])
            .map(|s| s.to_uppercase() == "WITHSCORES")
            .unwrap_or(false);

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let items = zset.rev_range(start, stop);
                format_range_result(items, withscores)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

fn parse_score_bound(arg: &RespValue, _default: f64) -> Option<f64> {
    let s = arg.to_string_lossy()?;
    match s.as_str() {
        "-inf" => Some(f64::NEG_INFINITY),
        "+inf" | "inf" => Some(f64::INFINITY),
        _ if s.starts_with('(') => s[1..].parse().ok(),
        _ => s.parse().ok(),
    }
}

pub async fn cmd_zrangebyscore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrangebyscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let min = match parse_score_bound(&args[1], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let max = match parse_score_bound(&args[2], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i]).map(|s| s.to_uppercase()).unwrap_or_default();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                if i + 2 < args.len() {
                    offset = arg_to_i64(&args[i + 1]).unwrap_or(0) as usize;
                    count = arg_to_i64(&args[i + 2]).unwrap_or(-1) as usize;
                    i += 2;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let items: Vec<_> = zset.range_by_score(min, max)
                    .into_iter()
                    .skip(offset)
                    .take(count)
                    .collect();
                format_range_result(items, withscores)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrevrangebyscore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrevrangebyscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    // Note: for ZREVRANGEBYSCORE, the order is max, min
    let max = match parse_score_bound(&args[1], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let min = match parse_score_bound(&args[2], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i]).map(|s| s.to_uppercase()).unwrap_or_default();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                if i + 2 < args.len() {
                    offset = arg_to_i64(&args[i + 1]).unwrap_or(0) as usize;
                    count = arg_to_i64(&args[i + 2]).unwrap_or(-1) as usize;
                    i += 2;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let mut items: Vec<_> = zset.range_by_score(min, max).into_iter().collect();
                items.reverse();
                let items: Vec<_> = items.into_iter().skip(offset).take(count).collect();
                format_range_result(items, withscores)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrangebylex(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrangebylex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let (min, min_inclusive) = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let (max, max_inclusive) = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let items = zset.range_by_lex(&min, min_inclusive, &max, max_inclusive);
                let resp: Vec<RespValue> = items.into_iter()
                    .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrevrangebylex(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrevrangebylex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    // Note: reversed order for REV
    let (max, max_inclusive) = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let (min, min_inclusive) = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let mut items = zset.range_by_lex(&min, min_inclusive, &max, max_inclusive);
                items.reverse();
                let resp: Vec<RespValue> = items.into_iter()
                    .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

fn parse_lex_bound(arg: &RespValue) -> Option<(Vec<u8>, bool)> {
    let s = arg.to_string_lossy()?;
    match s.as_str() {
        "-" => Some((vec![], true)),   // minimum
        "+" => Some((vec![], true)),   // maximum (empty = unbounded)
        _ if s.starts_with('[') => Some((s[1..].as_bytes().to_vec(), true)),
        _ if s.starts_with('(') => Some((s[1..].as_bytes().to_vec(), false)),
        _ => None,
    }
}

pub async fn cmd_zincrby(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zincrby");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let delta = match arg_to_f64(&args[1]) {
        Some(s) if !s.is_nan() => s,
        _ => return RespValue::error("ERR value is not a valid float"),
    };
    let member = match arg_to_bytes(&args[2]) {
        Some(m) => m.to_vec(),
        None => return RespValue::error("ERR invalid member"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let zset = match get_or_create_zset(db, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let new_score = zset.incr_by(member, delta);
    RespValue::bulk_string(format!("{new_score}").into_bytes())
}

pub async fn cmd_zunionstore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    zstore_op(args, store, client, "zunionstore").await
}

pub async fn cmd_zinterstore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    zstore_op(args, store, client, "zinterstore").await
}

async fn zstore_op(args: &[RespValue], store: &SharedStore, client: &ClientState, cmd: &str) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count(cmd);
    }
    let dest = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let numkeys = match arg_to_i64(&args[1]) {
        Some(n) if n > 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if args.len() < 2 + numkeys {
        return wrong_arg_count(cmd);
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Collect all sorted sets
    let mut zsets: Vec<Vec<(Vec<u8>, f64)>> = Vec::new();
    for i in 0..numkeys {
        let key = match arg_to_string(&args[2 + i]) {
            Some(k) => k,
            None => { zsets.push(vec![]); continue; }
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    zsets.push(zset.iter().map(|(m, s)| (m.to_vec(), s)).collect());
                }
                _ => return wrong_type_error(),
            },
            None => zsets.push(vec![]),
        }
    }

    let mut result = RedisSortedSet::new();

    if cmd == "zunionstore" {
        use std::collections::HashMap;
        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
        for zset in &zsets {
            for (member, score) in zset {
                *scores.entry(member.clone()).or_insert(0.0) += score;
            }
        }
        for (member, score) in scores {
            result.add(member, score);
        }
    } else {
        // zinterstore
        if let Some(first) = zsets.first() {
            for (member, score) in first {
                let mut total_score = *score;
                let mut in_all = true;
                for zset in &zsets[1..] {
                    if let Some((_, s)) = zset.iter().find(|(m, _)| m == member) {
                        total_score += s;
                    } else {
                        in_all = false;
                        break;
                    }
                }
                if in_all {
                    result.add(member.clone(), total_score);
                }
            }
        }
    }

    let len = result.len() as i64;
    db.set(dest, Entry::new(RedisValue::SortedSet(result)));
    RespValue::integer(len)
}

pub async fn cmd_zrandmember(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("zrandmember");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::null_bulk_string(),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                use rand::seq::IteratorRandom;
                let mut rng = rand::thread_rng();
                let items: Vec<_> = zset.iter().collect();
                if items.is_empty() {
                    return RespValue::null_bulk_string();
                }
                match items.into_iter().choose(&mut rng) {
                    Some((m, _)) => RespValue::bulk_string(m.to_vec()),
                    None => RespValue::null_bulk_string(),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::null_bulk_string(),
    }
}

pub async fn cmd_zscan(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("zscan");
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
            RedisValue::SortedSet(zset) => {
                let mut result = Vec::new();
                for (member, score) in zset.iter() {
                    result.push(RespValue::bulk_string(member.to_vec()));
                    result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
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

pub async fn cmd_zpopmin(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("zpopmin");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let count = if args.len() > 1 {
        arg_to_i64(&args[1]).unwrap_or(1).max(0) as usize
    } else {
        1
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::SortedSet(zset) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    match zset.pop_min() {
                        Some((member, score)) => {
                            result.push(RespValue::bulk_string(member));
                            result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
                        }
                        None => break,
                    }
                }
                RespValue::array(result)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zpopmax(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("zpopmax");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let count = if args.len() > 1 {
        arg_to_i64(&args[1]).unwrap_or(1).max(0) as usize
    } else {
        1
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::SortedSet(zset) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    match zset.pop_max() {
                        Some((member, score)) => {
                            result.push(RespValue::bulk_string(member));
                            result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
                        }
                        None => break,
                    }
                }
                RespValue::array(result)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zmscore(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("zmscore");
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
            RedisValue::SortedSet(zset) => {
                let results: Vec<RespValue> = args[1..]
                    .iter()
                    .map(|arg| {
                        if let Some(member) = arg_to_bytes(arg) {
                            match zset.score(member) {
                                Some(s) => RespValue::bulk_string(format!("{s}").into_bytes()),
                                None => RespValue::null_bulk_string(),
                            }
                        } else {
                            RespValue::null_bulk_string()
                        }
                    })
                    .collect();
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

pub async fn cmd_zlexcount(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zlexcount");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let (min, min_inclusive) = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let (max, max_inclusive) = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let items = zset.range_by_lex(&min, min_inclusive, &max, max_inclusive);
                RespValue::integer(items.len() as i64)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}
