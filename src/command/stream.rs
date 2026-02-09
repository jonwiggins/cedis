use crate::command::{arg_to_bytes, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::stream::{RedisStream, StreamEntryId};

/// Get or create a stream at the given key. Returns an error RespValue if the
/// key exists but holds a different type.
fn get_or_create_stream<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut RedisStream, RespValue> {
    if !db.exists(key) {
        let stream = RedisStream::new();
        db.set(key.to_string(), Entry::new(RedisValue::Stream(stream)));
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(s) => Ok(s),
            _ => Err(wrong_type_error()),
        },
        None => unreachable!(),
    }
}

/// Convert a stream entry (ID + fields) into a RESP array: [id, [f1, v1, f2, v2, ...]]
fn entry_to_resp(id: &StreamEntryId, fields: &[(Vec<u8>, Vec<u8>)]) -> RespValue {
    let id_str = id.to_string();
    let mut field_values = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        field_values.push(RespValue::bulk_string(f.clone()));
        field_values.push(RespValue::bulk_string(v.clone()));
    }
    RespValue::array(vec![
        RespValue::bulk_string(id_str),
        RespValue::array(field_values),
    ])
}

/// Parse a range bound ID string. "-" means minimum, "+" means maximum.
fn parse_range_bound(s: &str, is_min: bool) -> Option<StreamEntryId> {
    match s {
        "-" => Some(StreamEntryId::new(0, 0)),
        "+" => Some(StreamEntryId::new(u64::MAX, u64::MAX)),
        other => {
            if let Some(id) = StreamEntryId::parse(other) {
                Some(id)
            } else if let Ok(ms) = other.parse::<u64>() {
                // If only a timestamp is given, fill in seq
                if is_min {
                    Some(StreamEntryId::new(ms, 0))
                } else {
                    Some(StreamEntryId::new(ms, u64::MAX))
                }
            } else {
                None
            }
        }
    }
}

/// XADD key [MAXLEN [~] count] id_or_star field value [field value ...]
pub async fn cmd_xadd(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    // Minimum: key id field value = 4 args
    if args.len() < 4 {
        return wrong_arg_count("xadd");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut idx = 1;
    let mut maxlen: Option<usize> = None;

    // Check for MAXLEN option
    if let Some(opt) = arg_to_string(&args[idx]) {
        if opt.eq_ignore_ascii_case("MAXLEN") {
            idx += 1;
            if idx >= args.len() {
                return wrong_arg_count("xadd");
            }
            // Check for optional ~ (approximate trimming, we treat same as exact)
            if let Some(tilde) = arg_to_string(&args[idx]) {
                if tilde == "~" {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xadd");
                    }
                }
            }
            match arg_to_i64(&args[idx]) {
                Some(n) if n >= 0 => {
                    maxlen = Some(n as usize);
                    idx += 1;
                }
                _ => return RespValue::error("ERR value is not an integer or out of range"),
            }
        }
    }

    // Next arg is the ID
    if idx >= args.len() {
        return wrong_arg_count("xadd");
    }
    let id_str = match arg_to_string(&args[idx]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid stream ID"),
    };
    idx += 1;

    // Remaining args are field-value pairs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return wrong_arg_count("xadd");
    }

    let mut fields = Vec::with_capacity(remaining.len() / 2);
    for pair in remaining.chunks(2) {
        let field = match arg_to_bytes(&pair[0]) {
            Some(f) => f.to_vec(),
            None => return RespValue::error("ERR invalid field"),
        };
        let value = match arg_to_bytes(&pair[1]) {
            Some(v) => v.to_vec(),
            None => return RespValue::error("ERR invalid value"),
        };
        fields.push((field, value));
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let stream = match get_or_create_stream(db, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let id_arg = if id_str == "*" { None } else { Some(id_str.as_str()) };
    let entry_id = stream.add(id_arg, fields);

    // Apply MAXLEN trimming if specified
    if let Some(ml) = maxlen {
        stream.trim_maxlen(ml);
    }

    RespValue::bulk_string(entry_id.to_string())
}

/// XLEN key
pub async fn cmd_xlen(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() != 1 {
        return wrong_arg_count("xlen");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(s) => RespValue::integer(s.len() as i64),
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

/// XRANGE key start end [COUNT count]
pub async fn cmd_xrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xrange");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let start_str = match arg_to_string(&args[1]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid start ID"),
    };

    let end_str = match arg_to_string(&args[2]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid end ID"),
    };

    let start = match parse_range_bound(&start_str, true) {
        Some(id) => id,
        None => return RespValue::error("ERR Invalid stream ID specified as stream command argument"),
    };

    let end = match parse_range_bound(&end_str, false) {
        Some(id) => id,
        None => return RespValue::error("ERR Invalid stream ID specified as stream command argument"),
    };

    let mut count: Option<usize> = None;
    if args.len() >= 5 {
        if let Some(opt) = arg_to_string(&args[3]) {
            if opt.eq_ignore_ascii_case("COUNT") {
                match arg_to_i64(&args[4]) {
                    Some(n) if n >= 0 => count = Some(n as usize),
                    _ => return RespValue::error("ERR value is not an integer or out of range"),
                }
            }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(s) => {
                let entries = s.range(&start, &end);
                let limited: Vec<_> = match count {
                    Some(c) => entries.into_iter().take(c).collect(),
                    None => entries,
                };
                let resp_entries: Vec<RespValue> = limited
                    .iter()
                    .map(|(id, fields)| entry_to_resp(id, fields))
                    .collect();
                RespValue::array(resp_entries)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

/// XREVRANGE key end start [COUNT count]
pub async fn cmd_xrevrange(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xrevrange");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Note: XREVRANGE takes end first, then start (opposite of XRANGE)
    let end_str = match arg_to_string(&args[1]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid end ID"),
    };

    let start_str = match arg_to_string(&args[2]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid start ID"),
    };

    let end = match parse_range_bound(&end_str, false) {
        Some(id) => id,
        None => return RespValue::error("ERR Invalid stream ID specified as stream command argument"),
    };

    let start = match parse_range_bound(&start_str, true) {
        Some(id) => id,
        None => return RespValue::error("ERR Invalid stream ID specified as stream command argument"),
    };

    let mut count: Option<usize> = None;
    if args.len() >= 5 {
        if let Some(opt) = arg_to_string(&args[3]) {
            if opt.eq_ignore_ascii_case("COUNT") {
                match arg_to_i64(&args[4]) {
                    Some(n) if n >= 0 => count = Some(n as usize),
                    _ => return RespValue::error("ERR value is not an integer or out of range"),
                }
            }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(s) => {
                let entries = s.rev_range(&end, &start);
                let limited: Vec<_> = match count {
                    Some(c) => entries.into_iter().take(c).collect(),
                    None => entries,
                };
                let resp_entries: Vec<RespValue> = limited
                    .iter()
                    .map(|(id, fields)| entry_to_resp(id, fields))
                    .collect();
                RespValue::array(resp_entries)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::array(vec![]),
    }
}

/// XREAD [COUNT count] STREAMS key [key ...] id [id ...]
/// Non-blocking only.
pub async fn cmd_xread(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xread");
    }

    let mut idx = 0;
    let mut count: Option<usize> = None;

    // Parse optional COUNT
    while idx < args.len() {
        if let Some(opt) = arg_to_string(&args[idx]) {
            if opt.eq_ignore_ascii_case("COUNT") {
                idx += 1;
                if idx >= args.len() {
                    return wrong_arg_count("xread");
                }
                match arg_to_i64(&args[idx]) {
                    Some(n) if n >= 0 => {
                        count = Some(n as usize);
                        idx += 1;
                    }
                    _ => return RespValue::error("ERR value is not an integer or out of range"),
                }
            } else if opt.eq_ignore_ascii_case("BLOCK") {
                // We only support non-blocking; skip BLOCK argument but don't block
                idx += 1;
                if idx >= args.len() {
                    return wrong_arg_count("xread");
                }
                // Skip the timeout value
                idx += 1;
            } else if opt.eq_ignore_ascii_case("STREAMS") {
                idx += 1;
                break;
            } else {
                return RespValue::error("ERR Unrecognized XREAD option");
            }
        } else {
            return RespValue::error("ERR invalid argument");
        }
    }

    // After STREAMS keyword, we have keys followed by IDs
    // The number of keys == number of IDs, and they split evenly
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return wrong_arg_count("xread");
    }

    let half = remaining.len() / 2;
    let key_args = &remaining[..half];
    let id_args = &remaining[half..];

    let mut keys = Vec::with_capacity(half);
    for arg in key_args {
        match arg_to_string(arg) {
            Some(k) => keys.push(k),
            None => return RespValue::error("ERR invalid key"),
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let mut results = Vec::with_capacity(half);
    let mut any_results = false;

    for (i, key) in keys.iter().enumerate() {
        let id_str = match arg_to_string(&id_args[i]) {
            Some(s) => s,
            None => return RespValue::error("ERR invalid stream ID"),
        };

        match db.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::Stream(stream) => {
                    // "$" means read entries after the stream's last ID
                    let start_id = if id_str == "$" {
                        let last = stream.last_id();
                        StreamEntryId::new(last.ms, last.seq + 1)
                    } else {
                        match StreamEntryId::parse(&id_str) {
                            Some(id) => {
                                // XREAD returns entries with IDs strictly greater than the given ID
                                StreamEntryId::new(id.ms, id.seq + 1)
                            }
                            None => return RespValue::error("ERR Invalid stream ID specified as stream command argument"),
                        }
                    };

                    let end_id = StreamEntryId::new(u64::MAX, u64::MAX);
                    let entries = stream.range(&start_id, &end_id);
                    let limited: Vec<_> = match count {
                        Some(c) => entries.into_iter().take(c).collect(),
                        None => entries,
                    };

                    if !limited.is_empty() {
                        any_results = true;
                    }

                    let resp_entries: Vec<RespValue> = limited
                        .iter()
                        .map(|(id, fields)| entry_to_resp(id, fields))
                        .collect();

                    results.push(RespValue::array(vec![
                        RespValue::bulk_string(key.as_bytes()),
                        RespValue::array(resp_entries),
                    ]));
                }
                _ => return wrong_type_error(),
            },
            None => {
                // Key doesn't exist — return empty array for this stream
                results.push(RespValue::array(vec![
                    RespValue::bulk_string(key.as_bytes()),
                    RespValue::array(vec![]),
                ]));
            }
        }
    }

    if any_results {
        RespValue::array(results)
    } else {
        RespValue::null_array()
    }
}

/// XTRIM key MAXLEN [~] count
pub async fn cmd_xtrim(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xtrim");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut idx = 1;

    // Expect MAXLEN keyword
    match arg_to_string(&args[idx]) {
        Some(opt) if opt.eq_ignore_ascii_case("MAXLEN") => {
            idx += 1;
        }
        _ => return RespValue::error("ERR syntax error"),
    }

    if idx >= args.len() {
        return wrong_arg_count("xtrim");
    }

    // Check for optional ~ (approximate trimming, treated same as exact)
    if let Some(tilde) = arg_to_string(&args[idx]) {
        if tilde == "~" {
            idx += 1;
            if idx >= args.len() {
                return wrong_arg_count("xtrim");
            }
        }
    }

    let maxlen = match arg_to_i64(&args[idx]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(s) => {
                let trimmed = s.trim_maxlen(maxlen);
                RespValue::integer(trimmed as i64)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}
