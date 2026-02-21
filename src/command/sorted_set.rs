use crate::command::{
    arg_to_bytes, arg_to_f64, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error,
};
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::sorted_set::RedisSortedSet;
use std::time::Duration;

fn get_or_create_zset<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisSortedSet, RespValue> {
    if !db.exists(key) {
        db.set(
            key.to_string(),
            Entry::new(RedisValue::SortedSet(RedisSortedSet::new())),
        );
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::SortedSet(z) => Ok(z),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

pub async fn cmd_zadd(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
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
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GT" => {
                gt = true;
                i += 1;
            }
            "LT" => {
                lt = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            "INCR" => {
                incr = true;
                i += 1;
            }
            _ => break,
        }
    }

    // Validate flag combinations
    if nx && xx {
        return RespValue::error("ERR XX and NX options at the same time are not compatible");
    }
    if nx && (gt || lt) {
        return RespValue::error("ERR GT, LT, and NX options at the same time are not compatible");
    }
    if gt && lt {
        return RespValue::error("ERR GT and LT options at the same time are not compatible");
    }

    // Remaining args are score-member pairs
    let pairs = &args[i..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return RespValue::error("ERR syntax error");
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

        // Check existing state before creating key
        let (exists, old_score) = match db.get(&key) {
            Some(entry) => match &entry.value {
                crate::types::RedisValue::SortedSet(z) => (z.contains(&member), z.score(&member)),
                _ => return wrong_type_error(),
            },
            None => (false, None),
        };

        if nx && exists {
            return RespValue::null_bulk_string();
        }
        if xx && !exists {
            return RespValue::null_bulk_string();
        }

        // Check GT/LT before modifying
        if let Some(old) = old_score {
            let new_score = old + score;
            if new_score.is_nan() {
                return RespValue::error("ERR resulting score is not a number (NaN)");
            }
            if gt && new_score <= old {
                return RespValue::null_bulk_string();
            }
            if lt && new_score >= old {
                return RespValue::null_bulk_string();
            }
        }

        let zset = match get_or_create_zset(db, &key) {
            Ok(z) => z,
            Err(e) => return e,
        };

        let new_score = zset.incr_by(member, score);
        if new_score.is_nan() {
            return RespValue::error("ERR resulting score is not a number (NaN)");
        }
        drop(store);
        let mut watcher = key_watcher.write().await;
        watcher.notify(&key);
        RespValue::bulk_string(format!("{new_score}").into_bytes())
    } else {
        // Pre-validate all scores before modifying anything (atomicity)
        let mut parsed_pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(pairs.len() / 2);
        for pair in pairs.chunks(2) {
            let score = match arg_to_f64(&pair[0]) {
                Some(s) if !s.is_nan() => s,
                _ => return RespValue::error("ERR value is not a valid float"),
            };
            let member = match arg_to_bytes(&pair[1]) {
                Some(m) => m.to_vec(),
                None => continue,
            };
            parsed_pairs.push((score, member));
        }

        // For XX mode on non-existing key, no-op
        if xx && !db.exists(&key) {
            return RespValue::integer(0);
        }

        // Check type before creating
        if let Some(entry) = db.get(&key)
            && !matches!(&entry.value, RedisValue::SortedSet(_))
        {
            return wrong_type_error();
        }

        let zset = match get_or_create_zset(db, &key) {
            Ok(z) => z,
            Err(e) => return e,
        };

        let mut added = 0i64;
        let mut changed = 0i64;

        for (score, member) in parsed_pairs {
            let exists = zset.contains(&member);
            let old_score = zset.score(&member);

            if nx && exists {
                continue;
            }
            if xx && !exists {
                continue;
            }

            // GT/LT checks
            if let Some(old) = old_score {
                if gt && score <= old {
                    continue;
                }
                if lt && score >= old {
                    continue;
                }
            }

            let is_new = zset.add(member, score);
            if is_new {
                added += 1;
            } else if old_score != Some(score) {
                changed += 1;
            }
        }

        // Auto-delete key if sorted set is empty after XX filtering created it empty
        if zset.is_empty() {
            db.del(&key);
        }

        let result = if ch { added + changed } else { added };
        if added > 0 {
            drop(store);
            let mut watcher = key_watcher.write().await;
            watcher.notify(&key);
        }
        RespValue::integer(result)
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

    // Check type first
    let is_zset =
        matches!(db.get(&key), Some(entry) if matches!(&entry.value, RedisValue::SortedSet(_)));
    if !is_zset {
        return match db.get(&key) {
            Some(_) => wrong_type_error(),
            None => RespValue::integer(0),
        };
    }

    let mut removed = 0i64;
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for arg in &args[1..] {
            if let Some(member) = arg_to_bytes(arg)
                && zset.remove(member)
            {
                removed += 1;
            }
        }
    }

    // Auto-delete key when sorted set becomes empty
    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::integer(removed)
}

pub async fn cmd_zscore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
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
    if args.len() < 2 || args.len() > 3 {
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
    let withscore = if args.len() == 3 {
        match arg_to_string(&args[2]) {
            Some(s) if s.eq_ignore_ascii_case("WITHSCORE") => true,
            _ => return RespValue::error("ERR syntax error"),
        }
    } else {
        false
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => match zset.rank(member) {
                Some(r) => {
                    if withscore {
                        let score = zset.score(member).unwrap_or(0.0);
                        RespValue::Array(Some(vec![
                            RespValue::integer(r as i64),
                            RespValue::bulk_string(format!("{score}").into_bytes()),
                        ]))
                    } else {
                        RespValue::integer(r as i64)
                    }
                }
                None => {
                    if withscore {
                        RespValue::null_array()
                    } else {
                        RespValue::null_bulk_string()
                    }
                }
            },
            _ => wrong_type_error(),
        },
        None => {
            if withscore {
                RespValue::null_array()
            } else {
                RespValue::null_bulk_string()
            }
        }
    }
}

pub async fn cmd_zrevrank(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 || args.len() > 3 {
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
    let withscore = if args.len() == 3 {
        match arg_to_string(&args[2]) {
            Some(s) if s.eq_ignore_ascii_case("WITHSCORE") => true,
            _ => return RespValue::error("ERR syntax error"),
        }
    } else {
        false
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => match zset.rev_rank(member) {
                Some(r) => {
                    if withscore {
                        let score = zset.score(member).unwrap_or(0.0);
                        RespValue::Array(Some(vec![
                            RespValue::integer(r as i64),
                            RespValue::bulk_string(format!("{score}").into_bytes()),
                        ]))
                    } else {
                        RespValue::integer(r as i64)
                    }
                }
                None => {
                    if withscore {
                        RespValue::null_array()
                    } else {
                        RespValue::null_bulk_string()
                    }
                }
            },
            _ => wrong_type_error(),
        },
        None => {
            if withscore {
                RespValue::null_array()
            } else {
                RespValue::null_bulk_string()
            }
        }
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

pub async fn cmd_zcount(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zcount");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let (min_val, min_incl) = match parse_score_bound(&args[1], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max_val, max_incl) = match parse_score_bound(&args[2], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let count = zset
                    .range_by_score(min_val, max_val)
                    .into_iter()
                    .filter(|&(_, score)| {
                        let above_min = if min_incl {
                            score >= min_val
                        } else {
                            score > min_val
                        };
                        let below_max = if max_incl {
                            score <= max_val
                        } else {
                            score < max_val
                        };
                        above_min && below_max
                    })
                    .count();
                RespValue::integer(count as i64)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

/// Format a score like Redis does (%.17g equivalent)
fn format_score(score: f64) -> String {
    if score == 0.0 {
        return "0".to_string();
    }
    if score == f64::INFINITY {
        return "inf".to_string();
    }
    if score == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    // Check if it's an integer value
    if score.fract() == 0.0 && score.abs() < 1e18 {
        return format!("{}", score as i64);
    }
    // Use 17 significant digits like Redis's %.17g
    format!("{}", score)
}

fn format_range_result(items: Vec<(&[u8], f64)>, withscores: bool) -> RespValue {
    let mut result = Vec::new();
    for (member, score) in items {
        result.push(RespValue::bulk_string(member.to_vec()));
        if withscores {
            result.push(RespValue::bulk_string(format_score(score).into_bytes()));
        }
    }
    RespValue::array(result)
}

pub async fn cmd_zrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    if args.len() < 3 {
        return wrong_arg_count("zrange");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    // Parse optional flags
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset = 0usize;
    let mut limit_count: Option<usize> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "BYSCORE" => byscore = true,
            "BYLEX" => bylex = true,
            "REV" => rev = true,
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                if i + 2 < args.len() {
                    let off = arg_to_i64(&args[i + 1]).unwrap_or(0);
                    let cnt = arg_to_i64(&args[i + 2]).unwrap_or(-1);
                    if off < 0 {
                        return RespValue::array(vec![]);
                    }
                    limit_offset = off as usize;
                    limit_count = Some(if cnt < 0 { usize::MAX } else { cnt as usize });
                    i += 2;
                }
            }
            _ => {}
        }
        i += 1;
    }

    if byscore && bylex {
        return RespValue::error("ERR BYSCORE and BYLEX options are not compatible");
    }
    if (byscore || bylex) && limit_count.is_none() {
        // LIMIT is optional; if not specified, return all matches
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if byscore {
        // Score-based range
        let (min_arg, max_arg) = if rev {
            (&args[2], &args[1])
        } else {
            (&args[1], &args[2])
        };
        let (min_val, min_incl) = match parse_score_bound(min_arg, f64::NEG_INFINITY) {
            Some(s) => s,
            None => return RespValue::error("ERR min or max is not a float"),
        };
        let (max_val, max_incl) = match parse_score_bound(max_arg, f64::INFINITY) {
            Some(s) => s,
            None => return RespValue::error("ERR min or max is not a float"),
        };

        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    let items: Vec<(&[u8], f64)> = zset
                        .range_by_score(min_val, max_val)
                        .into_iter()
                        .filter(|&(_, score)| {
                            let above_min = if min_incl {
                                score >= min_val
                            } else {
                                score > min_val
                            };
                            let below_max = if max_incl {
                                score <= max_val
                            } else {
                                score < max_val
                            };
                            above_min && below_max
                        })
                        .collect();
                    let items: Vec<(&[u8], f64)> = if rev {
                        items.into_iter().rev().collect()
                    } else {
                        items
                    };
                    let items: Vec<(&[u8], f64)> = items
                        .into_iter()
                        .skip(limit_offset)
                        .take(limit_count.unwrap_or(usize::MAX))
                        .collect();
                    format_range_result(items, withscores)
                }
                _ => wrong_type_error(),
            },
            None => RespValue::array(vec![]),
        }
    } else if bylex {
        // Lex-based range
        let (min_arg, max_arg) = if rev {
            (&args[2], &args[1])
        } else {
            (&args[1], &args[2])
        };
        let min_bound = match parse_lex_bound(min_arg) {
            Some(b) => b,
            None => return RespValue::error("ERR min or max not valid string range item"),
        };
        let max_bound = match parse_lex_bound(max_arg) {
            Some(b) => b,
            None => return RespValue::error("ERR min or max not valid string range item"),
        };

        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    let items: Vec<RespValue> = if rev {
                        zset.iter()
                            .rev()
                            .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                            .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                            .skip(limit_offset)
                            .take(limit_count.unwrap_or(usize::MAX))
                            .collect()
                    } else {
                        zset.iter()
                            .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                            .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                            .skip(limit_offset)
                            .take(limit_count.unwrap_or(usize::MAX))
                            .collect()
                    };
                    RespValue::array(items)
                }
                _ => wrong_type_error(),
            },
            None => RespValue::array(vec![]),
        }
    } else {
        // Index-based range (default)
        let start = match arg_to_i64(&args[1]) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let stop = match arg_to_i64(&args[2]) {
            Some(n) => n,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };

        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    let items = if rev {
                        zset.rev_range(start, stop)
                    } else {
                        zset.range(start, stop)
                    };
                    format_range_result(items, withscores)
                }
                _ => wrong_type_error(),
            },
            None => RespValue::array(vec![]),
        }
    }
}

pub async fn cmd_zrevrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
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

/// Returns (value, inclusive)
fn parse_score_bound(arg: &RespValue, _default: f64) -> Option<(f64, bool)> {
    let s = arg.to_string_lossy()?;
    match s.as_str() {
        "-inf" => Some((f64::NEG_INFINITY, true)),
        "+inf" | "inf" => Some((f64::INFINITY, true)),
        _ if s.starts_with('(') => {
            let rest = &s[1..];
            match rest {
                "-inf" => Some((f64::NEG_INFINITY, false)),
                "+inf" | "inf" => Some((f64::INFINITY, false)),
                _ => rest.parse().ok().map(|v| (v, false)),
            }
        }
        _ => {
            let v: f64 = s.parse().ok()?;
            if v.is_nan() {
                return None;
            }
            Some((v, true))
        }
    }
}

pub async fn cmd_zrangebyscore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrangebyscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let (min_val, min_incl) = match parse_score_bound(&args[1], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max_val, max_incl) = match parse_score_bound(&args[2], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
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
                let items: Vec<_> = zset
                    .range_by_score(min_val, max_val)
                    .into_iter()
                    .filter(|&(_, score)| {
                        let above_min = if min_incl {
                            score >= min_val
                        } else {
                            score > min_val
                        };
                        let below_max = if max_incl {
                            score <= max_val
                        } else {
                            score < max_val
                        };
                        above_min && below_max
                    })
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

pub async fn cmd_zrevrangebyscore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrevrangebyscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    // Note: for ZREVRANGEBYSCORE, the order is max, min
    let (max_val, max_incl) = match parse_score_bound(&args[1], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (min_val, min_incl) = match parse_score_bound(&args[2], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
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
                let mut items: Vec<_> = zset
                    .range_by_score(min_val, max_val)
                    .into_iter()
                    .filter(|&(_, score)| {
                        let above_min = if min_incl {
                            score >= min_val
                        } else {
                            score > min_val
                        };
                        let below_max = if max_incl {
                            score <= max_val
                        } else {
                            score < max_val
                        };
                        above_min && below_max
                    })
                    .collect();
                items.reverse();
                let items: Vec<_> = items.into_iter().skip(offset).take(count).collect();
                format_range_result(items, withscores)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrangebylex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrangebylex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    let min_bound = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let max_bound = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        if opt == "LIMIT" && i + 2 < args.len() {
            let off = arg_to_i64(&args[i + 1]).unwrap_or(0);
            let cnt = arg_to_i64(&args[i + 2]).unwrap_or(-1);
            if off < 0 {
                return RespValue::array(vec![]);
            }
            offset = off as usize;
            count = if cnt < 0 { usize::MAX } else { cnt as usize };
            i += 2;
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let resp: Vec<RespValue> = zset
                    .iter()
                    .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                    .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                    .skip(offset)
                    .take(count)
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

pub async fn cmd_zrevrangebylex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zrevrangebylex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };

    // Note: reversed order for REV — args[1] is max, args[2] is min
    let max_bound = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let min_bound = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut offset = 0usize;
    let mut count = usize::MAX;
    let mut i = 3;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        if opt == "LIMIT" && i + 2 < args.len() {
            let off = arg_to_i64(&args[i + 1]).unwrap_or(0);
            let cnt = arg_to_i64(&args[i + 2]).unwrap_or(-1);
            if off < 0 {
                return RespValue::array(vec![]);
            }
            offset = off as usize;
            count = if cnt < 0 { usize::MAX } else { cnt as usize };
            i += 2;
        }
        i += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let mut items: Vec<_> = zset
                    .iter()
                    .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                    .collect();
                items.reverse();
                let resp: Vec<RespValue> = items
                    .into_iter()
                    .map(|(m, _)| RespValue::bulk_string(m.to_vec()))
                    .skip(offset)
                    .take(count)
                    .collect();
                RespValue::array(resp)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

#[derive(Clone)]
enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

fn parse_lex_bound(arg: &RespValue) -> Option<LexBound> {
    let s = arg.to_string_lossy()?;
    match s.as_str() {
        "-" => Some(LexBound::NegInf),
        "+" => Some(LexBound::PosInf),
        _ if s.starts_with('[') => Some(LexBound::Inclusive(s.as_bytes()[1..].to_vec())),
        _ if s.starts_with('(') => Some(LexBound::Exclusive(s.as_bytes()[1..].to_vec())),
        _ => None,
    }
}

fn lex_above_min(m: &[u8], min: &LexBound) -> bool {
    match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false, // nothing is above +inf
        LexBound::Inclusive(v) => m >= v.as_slice(),
        LexBound::Exclusive(v) => m > v.as_slice(),
    }
}

fn lex_below_max(m: &[u8], max: &LexBound) -> bool {
    match max {
        LexBound::NegInf => false, // nothing is below -inf
        LexBound::PosInf => true,
        LexBound::Inclusive(v) => m <= v.as_slice(),
        LexBound::Exclusive(v) => m < v.as_slice(),
    }
}

fn lex_in_range(m: &[u8], min: &LexBound, max: &LexBound) -> bool {
    lex_above_min(m, min) && lex_below_max(m, max)
}

pub async fn cmd_zremrangebyscore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zremrangebyscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let (min_val, min_incl) = match parse_score_bound(&args[1], f64::NEG_INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max_val, max_incl) = match parse_score_bound(&args[2], f64::INFINITY) {
        Some(s) => s,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let members_to_remove: Vec<Vec<u8>> = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => zset
                .range_by_score(min_val, max_val)
                .into_iter()
                .filter(|&(_, score)| {
                    let above_min = if min_incl {
                        score >= min_val
                    } else {
                        score > min_val
                    };
                    let below_max = if max_incl {
                        score <= max_val
                    } else {
                        score < max_val
                    };
                    above_min && below_max
                })
                .map(|(m, _)| m.to_vec())
                .collect(),
            _ => return wrong_type_error(),
        },
        None => return RespValue::integer(0),
    };

    let removed = members_to_remove.len() as i64;
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for m in &members_to_remove {
            zset.remove(m);
        }
    }

    // Auto-delete empty key
    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::integer(removed)
}

pub async fn cmd_zremrangebylex(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zremrangebylex");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let min_bound = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let max_bound = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let members_to_remove: Vec<Vec<u8>> = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => zset
                .iter()
                .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                .map(|(m, _)| m.to_vec())
                .collect(),
            _ => return wrong_type_error(),
        },
        None => return RespValue::integer(0),
    };

    let removed = members_to_remove.len() as i64;
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for m in &members_to_remove {
            zset.remove(m);
        }
    }

    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::integer(removed)
}

pub async fn cmd_zremrangebyrank(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zremrangebyrank");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };
    let start = match arg_to_i64(&args[1]) {
        Some(s) => s,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop = match arg_to_i64(&args[2]) {
        Some(s) => s,
        None => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let members_to_remove: Vec<Vec<u8>> = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => zset
                .range(start, stop)
                .into_iter()
                .map(|(m, _)| m.to_vec())
                .collect(),
            _ => return wrong_type_error(),
        },
        None => return RespValue::integer(0),
    };

    let removed = members_to_remove.len() as i64;
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for m in &members_to_remove {
            zset.remove(m);
        }
    }

    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::integer(removed)
}

pub async fn cmd_zincrby(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
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
    if new_score.is_nan() {
        return RespValue::error("ERR resulting score is not a number (NaN)");
    }
    RespValue::bulk_string(format!("{new_score}").into_bytes())
}

pub async fn cmd_zunionstore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    zstore_op(args, store, client, "zunionstore").await
}

pub async fn cmd_zinterstore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    zstore_op(args, store, client, "zinterstore").await
}

async fn zstore_op(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    cmd: &str,
) -> RespValue {
    // Delegate to the unified combine op which handles WEIGHTS/AGGREGATE/regular sets
    zset_combine_op(args, store, client, cmd, true).await
}

pub async fn cmd_zunion(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    zset_combine_op(args, store, client, "zunion", false).await
}

pub async fn cmd_zinter(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    zset_combine_op(args, store, client, "zinter", false).await
}

pub async fn cmd_zdiff(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    zset_combine_op(args, store, client, "zdiff", false).await
}

pub async fn cmd_zdiffstore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    zset_combine_op(args, store, client, "zdiffstore", true).await
}

async fn zset_combine_op(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    cmd: &str,
    is_store: bool,
) -> RespValue {
    let min_args = if is_store { 3 } else { 2 };
    if args.len() < min_args {
        return wrong_arg_count(cmd);
    }

    let arg_offset = if is_store { 1 } else { 0 };
    let dest = if is_store {
        match arg_to_string(&args[0]) {
            Some(k) => Some(k),
            None => return RespValue::error("ERR invalid key"),
        }
    } else {
        None
    };

    let base_cmd = cmd.trim_end_matches("store");
    let numkeys = match arg_to_i64(&args[arg_offset]) {
        Some(n) if n > 0 => n as usize,
        Some(0) => {
            return RespValue::error(format!(
                "ERR at least 1 input key is needed for '{cmd}' command"
            ));
        }
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if args.len() < arg_offset + 1 + numkeys {
        return RespValue::error("ERR syntax error");
    }

    // Parse optional WEIGHTS and AGGREGATE and WITHSCORES
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM";
    let mut withscores = false;
    let mut i = arg_offset + 1 + numkeys;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "WEIGHTS" => {
                for j in 0..numkeys {
                    if i + 1 + j < args.len() {
                        match arg_to_f64(&args[i + 1 + j]) {
                            Some(w) if !w.is_nan() => weights[j] = w,
                            _ => return RespValue::error("ERR weight value is not a valid float"),
                        }
                    }
                }
                i += numkeys;
            }
            "AGGREGATE" => {
                if i + 1 < args.len() {
                    let agg = arg_to_string(&args[i + 1])
                        .map(|s| s.to_uppercase())
                        .unwrap_or_default();
                    match agg.as_str() {
                        "SUM" => aggregate = "SUM",
                        "MIN" => aggregate = "MIN",
                        "MAX" => aggregate = "MAX",
                        _ => return RespValue::error("ERR syntax error"),
                    }
                    i += 1;
                }
            }
            "WITHSCORES" => {
                if is_store {
                    return RespValue::error("ERR syntax error");
                }
                withscores = true;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let mut store_lock = store.write().await;
    let db = store_lock.db(client.db_index);

    // Collect all sorted sets (also handle regular sets)
    let mut zsets: Vec<Vec<(Vec<u8>, f64)>> = Vec::new();
    for j in 0..numkeys {
        let key = match arg_to_string(&args[arg_offset + 1 + j]) {
            Some(k) => k,
            None => {
                zsets.push(vec![]);
                continue;
            }
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    zsets.push(zset.iter().map(|(m, s)| (m.to_vec(), s)).collect());
                }
                RedisValue::Set(set) => {
                    zsets.push(
                        set.members()
                            .into_iter()
                            .map(|m| (m.to_vec(), 1.0))
                            .collect(),
                    );
                }
                _ => return wrong_type_error(),
            },
            None => zsets.push(vec![]),
        }
    }

    let mut result = RedisSortedSet::new();

    if base_cmd == "zunion" {
        use std::collections::HashMap;
        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
        for (idx, zset) in zsets.iter().enumerate() {
            let w = weights[idx];
            for (member, score) in zset {
                let raw_weighted = score * w;
                let weighted = if raw_weighted.is_nan() {
                    0.0
                } else {
                    raw_weighted
                };
                let entry = scores.entry(member.clone()).or_insert(match aggregate {
                    "MIN" => f64::INFINITY,
                    "MAX" => f64::NEG_INFINITY,
                    _ => 0.0,
                });
                match aggregate {
                    "SUM" => {
                        let sum = *entry + weighted;
                        // Redis: inf + (-inf) = 0
                        *entry = if sum.is_nan() { 0.0 } else { sum };
                    }
                    "MIN" => {
                        if weighted < *entry {
                            *entry = weighted;
                        }
                    }
                    "MAX" => {
                        if weighted > *entry {
                            *entry = weighted;
                        }
                    }
                    _ => {
                        let sum = *entry + weighted;
                        *entry = if sum.is_nan() { 0.0 } else { sum };
                    }
                }
            }
        }
        for (member, score) in scores {
            result.add(member, score);
        }
    } else if base_cmd == "zinter" {
        if let Some(first) = zsets.first() {
            for (member, score) in first {
                let raw = score * weights[0];
                let mut agg_score = if raw.is_nan() { 0.0 } else { raw };
                let mut in_all = true;
                for (idx, zset) in zsets[1..].iter().enumerate() {
                    if let Some((_, s)) = zset.iter().find(|(m, _)| m == member) {
                        let raw_w = s * weights[idx + 1];
                        let weighted = if raw_w.is_nan() { 0.0 } else { raw_w };
                        match aggregate {
                            "SUM" => {
                                let sum = agg_score + weighted;
                                agg_score = if sum.is_nan() { 0.0 } else { sum };
                            }
                            "MIN" => {
                                if weighted < agg_score {
                                    agg_score = weighted;
                                }
                            }
                            "MAX" => {
                                if weighted > agg_score {
                                    agg_score = weighted;
                                }
                            }
                            _ => {
                                let sum = agg_score + weighted;
                                agg_score = if sum.is_nan() { 0.0 } else { sum };
                            }
                        }
                    } else {
                        in_all = false;
                        break;
                    }
                }
                if in_all {
                    result.add(member.clone(), agg_score);
                }
            }
        }
    } else {
        // zdiff: members in first set not in any other set
        if let Some(first) = zsets.first() {
            for (member, score) in first {
                let in_others = zsets[1..]
                    .iter()
                    .any(|zset| zset.iter().any(|(m, _)| m == member));
                if !in_others {
                    result.add(member.clone(), *score);
                }
            }
        }
    }

    if is_store {
        let len = result.len() as i64;
        let dest_key = dest.unwrap();
        if result.is_empty() {
            db.del(&dest_key);
        } else {
            db.set(dest_key, Entry::new(RedisValue::SortedSet(result)));
        }
        RespValue::integer(len)
    } else {
        let items: Vec<_> = result.iter().collect();
        format_range_result(items, withscores)
    }
}

pub async fn cmd_zintercard(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("zintercard");
    }
    let numkeys = match arg_to_i64(&args[0]) {
        Some(n) if n > 0 => n as usize,
        Some(0) => {
            return RespValue::error("ERR at least 1 input key is needed for 'zintercard' command");
        }
        _ => return RespValue::error("ERR numkeys can't be non-positive value"),
    };

    if args.len() < 1 + numkeys {
        return RespValue::error("ERR syntax error");
    }

    // Parse optional LIMIT
    let mut limit = 0usize; // 0 = unlimited
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        if opt == "LIMIT" {
            if i + 1 >= args.len() {
                return RespValue::error("ERR syntax error");
            }
            match arg_to_i64(&args[i + 1]) {
                Some(v) if v >= 0 => {
                    limit = v as usize;
                    i += 2;
                }
                _ => return RespValue::error("ERR LIMIT can't be negative"),
            }
        } else {
            return RespValue::error("ERR syntax error");
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Collect all sorted sets
    let mut zsets: Vec<Vec<Vec<u8>>> = Vec::new();
    for j in 0..numkeys {
        let key = match arg_to_string(&args[1 + j]) {
            Some(k) => k,
            None => {
                zsets.push(vec![]);
                continue;
            }
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::SortedSet(zset) => {
                    zsets.push(zset.iter().map(|(m, _)| m.to_vec()).collect());
                }
                RedisValue::Set(set) => {
                    zsets.push(set.members().into_iter().map(|m| m.to_vec()).collect());
                }
                _ => return wrong_type_error(),
            },
            None => return RespValue::integer(0), // empty set means intersection is 0
        }
    }

    if zsets.is_empty() {
        return RespValue::integer(0);
    }

    // Count intersection
    let mut count = 0usize;
    if let Some(first) = zsets.first() {
        for member in first {
            let in_all = zsets[1..]
                .iter()
                .all(|zset| zset.iter().any(|m| m == member));
            if in_all {
                count += 1;
                if limit > 0 && count >= limit {
                    break;
                }
            }
        }
    }

    RespValue::integer(count as i64)
}

pub async fn cmd_zmpop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("zmpop");
    }
    let numkeys = match arg_to_i64(&args[0]) {
        Some(n) if n > 0 => n as usize,
        Some(0) | Some(_) => return RespValue::error("ERR numkeys can't be non-positive value"),
        _ => return RespValue::error("ERR numkeys can't be non-positive value"),
    };

    if args.len() < 1 + numkeys + 1 {
        return RespValue::error("ERR syntax error");
    }

    let direction_str = match arg_to_string(&args[1 + numkeys]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR syntax error"),
    };
    let pop_min = match direction_str.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => return RespValue::error("ERR syntax error"),
    };

    let mut count = 1usize;
    let mut had_count = false;
    let mut i = 2 + numkeys;
    while i < args.len() {
        let opt = arg_to_string(&args[i])
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        if opt == "COUNT" {
            if had_count {
                return RespValue::error("ERR syntax error");
            }
            if i + 1 >= args.len() {
                return RespValue::error("ERR syntax error");
            }
            match arg_to_i64(&args[i + 1]) {
                Some(n) if n > 0 => count = n as usize,
                Some(0) => {
                    return RespValue::error("ERR count value of ZMPOP is not allowed to be zero");
                }
                _ => {
                    return RespValue::error(
                        "ERR count value of ZMPOP command is not an integer or out of range",
                    );
                }
            }
            had_count = true;
            i += 2;
        } else {
            return RespValue::error("ERR syntax error");
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Find first non-empty sorted set
    for j in 0..numkeys {
        let key = match arg_to_string(&args[1 + j]) {
            Some(k) => k,
            None => continue,
        };

        let is_zset =
            matches!(db.get(&key), Some(e) if matches!(&e.value, RedisValue::SortedSet(_)));
        if !is_zset {
            if db.get(&key).is_some() {
                return wrong_type_error();
            }
            continue;
        }

        let is_empty = matches!(db.get(&key), Some(e) if matches!(&e.value, RedisValue::SortedSet(z) if z.is_empty()));
        if is_empty {
            continue;
        }

        let mut elements = Vec::new();
        if let Some(entry) = db.get_mut(&key)
            && let RedisValue::SortedSet(zset) = &mut entry.value
        {
            for _ in 0..count {
                let popped = if pop_min {
                    zset.pop_min()
                } else {
                    zset.pop_max()
                };
                match popped {
                    Some((member, score)) => {
                        elements.push(RespValue::Array(Some(vec![
                            RespValue::bulk_string(member),
                            RespValue::bulk_string(format!("{score}").into_bytes()),
                        ])));
                    }
                    None => break,
                }
            }
        }

        // Auto-delete empty key
        if let Some(entry) = db.get(&key)
            && let RedisValue::SortedSet(z) = &entry.value
            && z.is_empty()
        {
            db.del(&key);
        }

        return RespValue::Array(Some(vec![
            RespValue::bulk_string(key.into_bytes()),
            RespValue::Array(Some(elements)),
        ]));
    }

    RespValue::null_array()
}

pub async fn cmd_zrandmember(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() || args.len() > 3 {
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
                if args.len() == 1 {
                    // Single random member
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
                } else {
                    let count = match arg_to_i64(&args[1]) {
                        Some(n) => n,
                        None => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    };
                    let with_scores = args.len() == 3;
                    let allow_dups = count < 0;
                    let abs_count = count.unsigned_abs() as usize;

                    if abs_count == 0 {
                        return RespValue::array(vec![]);
                    }

                    // Protect against OOM with extreme negative counts
                    if allow_dups && abs_count >= (i64::MAX as usize) / 2 {
                        return RespValue::error("ERR value is out of range");
                    }

                    use rand::seq::IteratorRandom;
                    let mut rng = rand::thread_rng();
                    let all_items: Vec<(Vec<u8>, f64)> =
                        zset.iter().map(|(m, s)| (m.to_vec(), s)).collect();

                    if all_items.is_empty() {
                        return RespValue::array(vec![]);
                    }

                    let mut result = Vec::new();
                    if allow_dups {
                        for _ in 0..abs_count {
                            let idx = rand::Rng::gen_range(&mut rng, 0..all_items.len());
                            let (m, s) = &all_items[idx];
                            result.push(RespValue::bulk_string(m.clone()));
                            if with_scores {
                                result.push(RespValue::bulk_string(format!("{}", s).into_bytes()));
                            }
                        }
                    } else {
                        let take = abs_count.min(all_items.len());
                        let indices: Vec<usize> =
                            (0..all_items.len()).choose_multiple(&mut rng, take);
                        for idx in indices {
                            let (m, s) = &all_items[idx];
                            result.push(RespValue::bulk_string(m.clone()));
                            if with_scores {
                                result.push(RespValue::bulk_string(format!("{}", s).into_bytes()));
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
                RespValue::array(vec![])
            } else {
                RespValue::null_bulk_string()
            }
        }
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

pub async fn cmd_zpopmin(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("zpopmin");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let count = if args.len() > 1 {
        match arg_to_i64(&args[1]) {
            Some(n) if n < 0 => return RespValue::error("ERR value must be positive"),
            Some(n) => n as usize,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let is_zset = matches!(db.get(&key), Some(e) if matches!(&e.value, RedisValue::SortedSet(_)));
    if !is_zset {
        return match db.get(&key) {
            Some(_) => wrong_type_error(),
            None => RespValue::array(vec![]),
        };
    }

    let mut result = Vec::new();
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for _ in 0..count {
            match zset.pop_min() {
                Some((member, score)) => {
                    result.push(RespValue::bulk_string(member));
                    result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
                }
                None => break,
            }
        }
    }

    // Auto-delete empty key
    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::array(result)
}

pub async fn cmd_zpopmax(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("zpopmax");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::array(vec![]),
    };
    let count = if args.len() > 1 {
        match arg_to_i64(&args[1]) {
            Some(n) if n < 0 => return RespValue::error("ERR value must be positive"),
            Some(n) => n as usize,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let is_zset = matches!(db.get(&key), Some(e) if matches!(&e.value, RedisValue::SortedSet(_)));
    if !is_zset {
        return match db.get(&key) {
            Some(_) => wrong_type_error(),
            None => RespValue::array(vec![]),
        };
    }

    let mut result = Vec::new();
    if let Some(entry) = db.get_mut(&key)
        && let RedisValue::SortedSet(zset) = &mut entry.value
    {
        for _ in 0..count {
            match zset.pop_max() {
                Some((member, score)) => {
                    result.push(RespValue::bulk_string(member));
                    result.push(RespValue::bulk_string(format!("{score}").into_bytes()));
                }
                None => break,
            }
        }
    }

    // Auto-delete empty key
    if let Some(entry) = db.get(&key)
        && let RedisValue::SortedSet(z) = &entry.value
        && z.is_empty()
    {
        db.del(&key);
    }

    RespValue::array(result)
}

pub async fn cmd_zmscore(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("zmscore");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => {
            let nulls: Vec<RespValue> = args[1..]
                .iter()
                .map(|_| RespValue::null_bulk_string())
                .collect();
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
            let nulls: Vec<RespValue> = args[1..]
                .iter()
                .map(|_| RespValue::null_bulk_string())
                .collect();
            RespValue::array(nulls)
        }
    }
}

pub async fn cmd_zlexcount(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("zlexcount");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::integer(0),
    };

    let min_bound = match parse_lex_bound(&args[1]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };
    let max_bound = match parse_lex_bound(&args[2]) {
        Some(b) => b,
        None => return RespValue::error("ERR min or max not valid string range item"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::SortedSet(zset) => {
                let count = zset
                    .iter()
                    .filter(|(m, _)| lex_in_range(m, &min_bound, &max_bound))
                    .count();
                RespValue::integer(count as i64)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

/// Try to pop from the first non-empty sorted set. If `pop_min` is true, pops min; otherwise pops max.
fn try_zpop_from_keys(
    db: &mut crate::store::Database,
    keys: &[String],
    pop_min: bool,
) -> Option<RespValue> {
    for key in keys {
        let is_zset = matches!(db.get(key), Some(e) if matches!(&e.value, RedisValue::SortedSet(z) if !z.is_empty()));
        if !is_zset {
            continue;
        }

        if let Some(entry) = db.get_mut(key)
            && let RedisValue::SortedSet(zset) = &mut entry.value
        {
            let popped = if pop_min {
                zset.pop_min()
            } else {
                zset.pop_max()
            };
            if let Some((member, score)) = popped {
                let empty = zset.is_empty();
                if empty {
                    db.del(key);
                }
                return Some(RespValue::Array(Some(vec![
                    RespValue::bulk_string(key.as_bytes().to_vec()),
                    RespValue::bulk_string(member),
                    RespValue::bulk_string(format!("{score}").into_bytes()),
                ])));
            }
        }
    }
    None
}

/// BZPOPMIN key [key ...] timeout — blocking pop min
pub async fn cmd_bzpopmin(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("bzpopmin");
    }
    let timeout = match arg_to_f64(&args[args.len() - 1]) {
        Some(t) if t < 0.0 => {
            return RespValue::error("ERR timeout is not a float or out of range");
        }
        Some(t) => t,
        None => return RespValue::error("ERR timeout is not a float or out of range"),
    };

    let keys: Vec<String> = args[..args.len() - 1]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    // Try immediate pop
    {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_zpop_from_keys(db, &keys, true) {
            return result;
        }
    }

    let timeout_dur = if timeout == 0.0 {
        Duration::from_secs(365 * 24 * 3600)
    } else {
        Duration::from_secs_f64(timeout)
    };

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

    if notified {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_zpop_from_keys(db, &keys, true) {
            return result;
        }
    }

    RespValue::null_array()
}

/// BZPOPMAX key [key ...] timeout — blocking pop max
pub async fn cmd_bzpopmax(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("bzpopmax");
    }
    let timeout = match arg_to_f64(&args[args.len() - 1]) {
        Some(t) if t < 0.0 => {
            return RespValue::error("ERR timeout is not a float or out of range");
        }
        Some(t) => t,
        None => return RespValue::error("ERR timeout is not a float or out of range"),
    };

    let keys: Vec<String> = args[..args.len() - 1]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    // Try immediate pop
    {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_zpop_from_keys(db, &keys, false) {
            return result;
        }
    }

    let timeout_dur = if timeout == 0.0 {
        Duration::from_secs(365 * 24 * 3600)
    } else {
        Duration::from_secs_f64(timeout)
    };

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

    if notified {
        let mut store = store.write().await;
        let db = store.db(client.db_index);
        if let Some(result) = try_zpop_from_keys(db, &keys, false) {
            return result;
        }
    }

    RespValue::null_array()
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
pub async fn cmd_bzmpop(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("bzmpop");
    }
    let _timeout = match arg_to_f64(&args[0]) {
        Some(t) if t < 0.0 => {
            return RespValue::error("ERR timeout is not a float or out of range");
        }
        Some(t) => t,
        None => return RespValue::error("ERR timeout is not a float or out of range"),
    };

    // Reuse ZMPOP logic with args shifted by 1 (skip timeout)
    cmd_zmpop(&args[1..], store, client).await
}
