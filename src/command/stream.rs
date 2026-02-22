use crate::command::{arg_to_bytes, arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::stream::{RedisStream, StreamEntryId};
use std::time::Duration;

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
    key_watcher: &SharedKeyWatcher,
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
    if let Some(opt) = arg_to_string(&args[idx])
        && opt.eq_ignore_ascii_case("MAXLEN")
    {
        idx += 1;
        if idx >= args.len() {
            return wrong_arg_count("xadd");
        }
        // Check for optional ~ (approximate trimming, we treat same as exact)
        if let Some(tilde) = arg_to_string(&args[idx])
            && tilde == "~"
        {
            idx += 1;
            if idx >= args.len() {
                return wrong_arg_count("xadd");
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
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
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

    let id_arg = if id_str == "*" {
        None
    } else {
        Some(id_str.as_str())
    };
    let entry_id = stream.add(id_arg, fields);

    // Apply MAXLEN trimming if specified
    if let Some(ml) = maxlen {
        stream.trim_maxlen(ml);
    }

    // Notify any blocked readers (XREADGROUP BLOCK, etc.)
    drop(store);
    {
        let mut watcher = key_watcher.write().await;
        watcher.notify(&key);
    }

    RespValue::bulk_string(entry_id.to_string())
}

/// XLEN key
pub async fn cmd_xlen(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
        None => {
            return RespValue::error("ERR Invalid stream ID specified as stream command argument");
        }
    };

    let end = match parse_range_bound(&end_str, false) {
        Some(id) => id,
        None => {
            return RespValue::error("ERR Invalid stream ID specified as stream command argument");
        }
    };

    let mut count: Option<usize> = None;
    if args.len() >= 5
        && let Some(opt) = arg_to_string(&args[3])
        && opt.eq_ignore_ascii_case("COUNT")
    {
        match arg_to_i64(&args[4]) {
            Some(n) if n >= 0 => count = Some(n as usize),
            _ => return RespValue::error("ERR value is not an integer or out of range"),
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
        None => {
            return RespValue::error("ERR Invalid stream ID specified as stream command argument");
        }
    };

    let start = match parse_range_bound(&start_str, true) {
        Some(id) => id,
        None => {
            return RespValue::error("ERR Invalid stream ID specified as stream command argument");
        }
    };

    let mut count: Option<usize> = None;
    if args.len() >= 5
        && let Some(opt) = arg_to_string(&args[3])
        && opt.eq_ignore_ascii_case("COUNT")
    {
        match arg_to_i64(&args[4]) {
            Some(n) if n >= 0 => count = Some(n as usize),
            _ => return RespValue::error("ERR value is not an integer or out of range"),
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

/// XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
pub async fn cmd_xread(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
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
                            None => {
                                return RespValue::error(
                                    "ERR Invalid stream ID specified as stream command argument",
                                );
                            }
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

/// XDEL key id [id ...]
pub async fn cmd_xdel(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("xdel");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut ids = Vec::new();
    for arg in &args[1..] {
        match arg_to_string(arg).and_then(|s| StreamEntryId::parse(&s)) {
            Some(id) => ids.push(id),
            None => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                );
            }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(s) => {
                let deleted = s.xdel(&ids);
                RespValue::integer(deleted as i64)
            }
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

/// XINFO STREAM|GROUPS|CONSUMERS|HELP key [...]
pub async fn cmd_xinfo(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("xinfo");
    }

    let sub = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match sub.as_str() {
        "STREAM" => {
            if args.len() < 2 {
                return wrong_arg_count("xinfo|stream");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };

            let mut store = store.write().await;
            let db = store.db(client.db_index);

            match db.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::Stream(s) => {
                        let mut result = vec![
                            RespValue::bulk_string(b"length".to_vec()),
                            RespValue::integer(s.len() as i64),
                            RespValue::bulk_string(b"radix-tree-keys".to_vec()),
                            RespValue::integer(1),
                            RespValue::bulk_string(b"radix-tree-nodes".to_vec()),
                            RespValue::integer(2),
                            RespValue::bulk_string(b"last-generated-id".to_vec()),
                            RespValue::bulk_string(s.last_id().to_string().into_bytes()),
                            RespValue::bulk_string(b"groups".to_vec()),
                            RespValue::integer(s.group_count() as i64),
                        ];
                        if let Some((id, fields)) = s.first_entry() {
                            result.push(RespValue::bulk_string(b"first-entry".to_vec()));
                            result.push(entry_to_resp(id, fields));
                        }
                        if let Some((id, fields)) = s.last_entry() {
                            result.push(RespValue::bulk_string(b"last-entry".to_vec()));
                            result.push(entry_to_resp(id, fields));
                        }
                        RespValue::array(result)
                    }
                    _ => wrong_type_error(),
                },
                None => RespValue::error("ERR no such key"),
            }
        }
        "GROUPS" => {
            if args.len() < 2 {
                return wrong_arg_count("xinfo|groups");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };

            let mut store = store.write().await;
            let db = store.db(client.db_index);

            match db.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::Stream(s) => {
                        let mut groups: Vec<(&String, &crate::types::stream::ConsumerGroup)> =
                            s.groups.iter().collect();
                        groups.sort_by_key(|(name, _)| (*name).clone());

                        let result: Vec<RespValue> = groups
                            .iter()
                            .map(|(_, group)| {
                                // Compute lag: entries in stream after last_delivered_id
                                let lag = if group.last_delivered_id == StreamEntryId::new(0, 0) {
                                    s.len() as i64
                                } else {
                                    let after = StreamEntryId::new(
                                        group.last_delivered_id.ms,
                                        group.last_delivered_id.seq + 1,
                                    );
                                    let end = StreamEntryId::new(u64::MAX, u64::MAX);
                                    s.range(&after, &end).len() as i64
                                };

                                RespValue::array(vec![
                                    RespValue::bulk_string(b"name".to_vec()),
                                    RespValue::bulk_string(group.name.as_bytes().to_vec()),
                                    RespValue::bulk_string(b"consumers".to_vec()),
                                    RespValue::integer(group.consumers.len() as i64),
                                    RespValue::bulk_string(b"pending".to_vec()),
                                    RespValue::integer(group.pel.len() as i64),
                                    RespValue::bulk_string(b"last-delivered-id".to_vec()),
                                    RespValue::bulk_string(
                                        group.last_delivered_id.to_string().into_bytes(),
                                    ),
                                    RespValue::bulk_string(b"entries-read".to_vec()),
                                    RespValue::integer(group.entries_read as i64),
                                    RespValue::bulk_string(b"lag".to_vec()),
                                    RespValue::integer(lag),
                                ])
                            })
                            .collect();
                        RespValue::array(result)
                    }
                    _ => wrong_type_error(),
                },
                None => RespValue::error("ERR no such key"),
            }
        }
        "CONSUMERS" => {
            if args.len() < 3 {
                return wrong_arg_count("xinfo|consumers");
            }
            let key = match arg_to_string(&args[1]) {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };
            let group_name = match arg_to_string(&args[2]) {
                Some(g) => g,
                None => return RespValue::error("ERR invalid group name"),
            };

            let mut store = store.write().await;
            let db = store.db(client.db_index);

            match db.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::Stream(s) => match s.get_group(&group_name) {
                        Some(group) => {
                            let now = crate::store::entry::now_millis();
                            let mut consumers: Vec<_> = group.consumers.values().collect();
                            consumers.sort_by_key(|c| &c.name);

                            let result: Vec<RespValue> = consumers
                                .iter()
                                .map(|consumer| {
                                    let idle = now.saturating_sub(consumer.seen_time);
                                    RespValue::array(vec![
                                        RespValue::bulk_string(b"name".to_vec()),
                                        RespValue::bulk_string(consumer.name.as_bytes().to_vec()),
                                        RespValue::bulk_string(b"pending".to_vec()),
                                        RespValue::integer(consumer.pending.len() as i64),
                                        RespValue::bulk_string(b"idle".to_vec()),
                                        RespValue::integer(idle as i64),
                                    ])
                                })
                                .collect();
                            RespValue::array(result)
                        }
                        None => RespValue::error("NOGROUP No such consumer group for key"),
                    },
                    _ => wrong_type_error(),
                },
                None => RespValue::error("ERR no such key"),
            }
        }
        "HELP" => RespValue::array(vec![
            RespValue::bulk_string(
                b"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_vec(),
            ),
            RespValue::bulk_string(b"CONSUMERS <key> <groupname>".to_vec()),
            RespValue::bulk_string(b"GROUPS <key>".to_vec()),
            RespValue::bulk_string(b"STREAM <key> [FULL [COUNT <count>]]".to_vec()),
            RespValue::bulk_string(b"HELP".to_vec()),
        ]),
        _ => RespValue::error("ERR unknown subcommand"),
    }
}

/// XTRIM key MAXLEN [~] count
pub async fn cmd_xtrim(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
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
    if let Some(tilde) = arg_to_string(&args[idx])
        && tilde == "~"
    {
        idx += 1;
        if idx >= args.len() {
            return wrong_arg_count("xtrim");
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

/// XGROUP CREATE|DESTROY|CREATECONSUMER|DELCONSUMER|SETID ...
pub async fn cmd_xgroup(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("xgroup");
    }

    let sub = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    match sub.as_str() {
        "CREATE" => cmd_xgroup_create(&args[1..], store, client).await,
        "DESTROY" => cmd_xgroup_destroy(&args[1..], store, client).await,
        "CREATECONSUMER" => cmd_xgroup_createconsumer(&args[1..], store, client).await,
        "DELCONSUMER" => cmd_xgroup_delconsumer(&args[1..], store, client).await,
        "SETID" => cmd_xgroup_setid(&args[1..], store, client).await,
        _ => RespValue::error(
            "ERR unknown subcommand or wrong number of arguments for 'XGROUP' command",
        ),
    }
}

/// XGROUP CREATE key group id|$ [MKSTREAM]
async fn cmd_xgroup_create(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xgroup|create");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let id_str = match arg_to_string(&args[2]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid stream ID"),
    };

    let mkstream = args.len() > 3
        && arg_to_string(&args[3])
            .map(|s| s.eq_ignore_ascii_case("MKSTREAM"))
            .unwrap_or(false);

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Check if key exists
    if !db.exists(&key) {
        if mkstream {
            let stream = RedisStream::new();
            db.set(key.clone(), Entry::new(RedisValue::Stream(stream)));
        } else {
            return RespValue::error(
                "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
            );
        }
    }

    let entry = match db.get_mut(&key) {
        Some(e) => e,
        None => unreachable!(),
    };

    match &mut entry.value {
        RedisValue::Stream(stream) => {
            let start_id = if id_str == "$" {
                stream.last_id().clone()
            } else if id_str == "0" || id_str == "0-0" {
                StreamEntryId::new(0, 0)
            } else {
                match StreamEntryId::parse(&id_str) {
                    Some(id) => id,
                    None => {
                        return RespValue::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        );
                    }
                }
            };

            match stream.create_group(&group_name, start_id) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::error(e),
            }
        }
        _ => wrong_type_error(),
    }
}

/// XGROUP DESTROY key group
async fn cmd_xgroup_destroy(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("xgroup|destroy");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => {
                if stream.destroy_group(&group_name) {
                    RespValue::integer(1)
                } else {
                    RespValue::integer(0)
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    }
}

/// XGROUP CREATECONSUMER key group consumer
async fn cmd_xgroup_createconsumer(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xgroup|createconsumer");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let consumer_name = match arg_to_string(&args[2]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid consumer name"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => match stream.get_group_mut(&group_name) {
                Some(group) => {
                    let created = group.create_consumer(&consumer_name);
                    RespValue::integer(if created { 1 } else { 0 })
                }
                None => RespValue::error("NOGROUP No such consumer group for key"),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    }
}

/// XGROUP DELCONSUMER key group consumer
async fn cmd_xgroup_delconsumer(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xgroup|delconsumer");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let consumer_name = match arg_to_string(&args[2]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid consumer name"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => match stream.get_group_mut(&group_name) {
                Some(group) => {
                    let deleted = group.delete_consumer(&consumer_name);
                    RespValue::integer(deleted as i64)
                }
                None => RespValue::error("NOGROUP No such consumer group for key"),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    }
}

/// XGROUP SETID key group id|$
async fn cmd_xgroup_setid(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xgroup|setid");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let id_str = match arg_to_string(&args[2]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid stream ID"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => {
                let new_id = if id_str == "$" {
                    stream.last_id().clone()
                } else if id_str == "0" || id_str == "0-0" {
                    StreamEntryId::new(0, 0)
                } else {
                    match StreamEntryId::parse(&id_str) {
                        Some(id) => id,
                        None => {
                            return RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            );
                        }
                    }
                };

                match stream.set_group_id(&group_name, new_id) {
                    Ok(()) => RespValue::ok(),
                    Err(e) => RespValue::error(e),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    }
}

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]
pub async fn cmd_xreadgroup(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
    key_watcher: &SharedKeyWatcher,
) -> RespValue {
    if args.len() < 6 {
        return wrong_arg_count("xreadgroup");
    }

    // Parse GROUP keyword
    let group_kw = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid argument"),
    };
    if group_kw != "GROUP" {
        return RespValue::error("ERR syntax error");
    }

    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let consumer_name = match arg_to_string(&args[2]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid consumer name"),
    };

    let mut idx = 3;
    let mut count: Option<usize> = None;
    let mut block: Option<u64> = None;
    let mut noack = false;

    // Parse optional COUNT, BLOCK, NOACK
    while idx < args.len() {
        if let Some(opt) = arg_to_string(&args[idx]) {
            let upper = opt.to_uppercase();
            match upper.as_str() {
                "COUNT" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xreadgroup");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n >= 0 => {
                            count = Some(n as usize);
                            idx += 1;
                        }
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    }
                }
                "BLOCK" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xreadgroup");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n >= 0 => {
                            block = Some(n as u64);
                            idx += 1;
                        }
                        _ => {
                            return RespValue::error(
                                "ERR timeout is not an integer or out of range",
                            );
                        }
                    }
                }
                "NOACK" => {
                    noack = true;
                    idx += 1;
                }
                "STREAMS" => {
                    idx += 1;
                    break;
                }
                _ => return RespValue::error("ERR Unrecognized XREADGROUP option"),
            }
        } else {
            return RespValue::error("ERR invalid argument");
        }
    }

    // After STREAMS keyword: keys followed by IDs
    let remaining = &args[idx..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return wrong_arg_count("xreadgroup");
    }

    let half = remaining.len() / 2;
    let key_args = &remaining[..half];
    let id_args = &remaining[half..];

    let mut keys = Vec::with_capacity(half);
    let mut ids = Vec::with_capacity(half);
    for i in 0..half {
        match arg_to_string(&key_args[i]) {
            Some(k) => keys.push(k),
            None => return RespValue::error("ERR invalid key"),
        }
        match arg_to_string(&id_args[i]) {
            Some(s) => ids.push(s),
            None => return RespValue::error("ERR invalid stream ID"),
        }
    }

    // First attempt: non-blocking read
    let result = xreadgroup_attempt(
        store,
        client,
        &group_name,
        &consumer_name,
        &keys,
        &ids,
        count,
        noack,
    )
    .await;
    match result {
        Err(resp) => return resp,
        Ok(Some(resp)) => return resp,
        Ok(None) => {
            // No results - check if we should block
        }
    }

    // If BLOCK specified and using ">" for at least one key, block and wait
    let uses_new = ids.iter().any(|id| id == ">");
    if let Some(timeout_ms) = block {
        if uses_new {
            let notify = {
                let mut watcher = key_watcher.write().await;
                watcher.register_many(&keys)
            };

            let timeout = if timeout_ms == 0 {
                Duration::from_secs(300) // Treat 0 as very long timeout
            } else {
                Duration::from_millis(timeout_ms)
            };

            tokio::select! {
                _ = notify.notified() => {
                    // Data may have arrived - retry
                    let result = xreadgroup_attempt(store, client, &group_name, &consumer_name, &keys, &ids, count, noack).await;
                    match result {
                        Err(resp) => resp,
                        Ok(Some(resp)) => resp,
                        Ok(None) => RespValue::null_array(),
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    // Timeout - clean up waiter
                    let mut watcher = key_watcher.write().await;
                    watcher.unregister_many(&keys, &notify);
                    RespValue::null_array()
                }
            }
        } else {
            RespValue::null_array()
        }
    } else {
        RespValue::null_array()
    }
}

/// Attempt a non-blocking XREADGROUP. Returns Ok(Some(resp)) if results, Ok(None) if empty, Err(resp) on error.
#[allow(clippy::too_many_arguments)]
async fn xreadgroup_attempt(
    store: &SharedStore,
    client: &ClientState,
    group_name: &str,
    consumer_name: &str,
    keys: &[String],
    ids: &[String],
    count: Option<usize>,
    noack: bool,
) -> Result<Option<RespValue>, RespValue> {
    let mut store_guard = store.write().await;
    let db = store_guard.db(client.db_index);

    let mut results = Vec::with_capacity(keys.len());
    let mut any_results = false;

    for (i, key) in keys.iter().enumerate() {
        let id_str = &ids[i];

        match db.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::Stream(stream) => {
                    match stream.read_group(group_name, consumer_name, id_str, count, noack) {
                        Ok(entries) => {
                            if !entries.is_empty() {
                                any_results = true;
                            }

                            let resp_entries: Vec<RespValue> = entries
                                .iter()
                                .map(|(id, fields)| {
                                    if fields.is_empty() {
                                        // Deleted entry - return nil
                                        RespValue::array(vec![
                                            RespValue::bulk_string(id.to_string()),
                                            RespValue::null_bulk_string(),
                                        ])
                                    } else {
                                        entry_to_resp(id, fields)
                                    }
                                })
                                .collect();

                            results.push(RespValue::array(vec![
                                RespValue::bulk_string(key.as_bytes()),
                                RespValue::array(resp_entries),
                            ]));
                        }
                        Err(e) => return Err(RespValue::error(e)),
                    }
                }
                _ => return Err(wrong_type_error()),
            },
            None => {
                // Key doesn't exist - check if group could exist
                // For non-existent keys with ">", return nothing
                // For non-existent keys with specific ID, return empty
                if id_str != ">" {
                    return Err(RespValue::error(format!(
                        "NOGROUP No such consumer group '{group_name}' for key"
                    )));
                }
            }
        }
    }

    if any_results {
        Ok(Some(RespValue::array(results)))
    } else {
        Ok(None)
    }
}

/// XACK key group id [id ...]
pub async fn cmd_xack(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("xack");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };

    let mut entry_ids = Vec::new();
    for arg in &args[2..] {
        match arg_to_string(arg).and_then(|s| StreamEntryId::parse(&s)) {
            Some(id) => entry_ids.push(id),
            None => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                );
            }
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => match stream.xack(&group_name, &entry_ids) {
                Ok(count) => RespValue::integer(count as i64),
                Err(e) => RespValue::error(e),
            },
            _ => wrong_type_error(),
        },
        None => RespValue::integer(0),
    }
}

/// XCLAIM key group consumer min-idle id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]
pub async fn cmd_xclaim(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 5 {
        return wrong_arg_count("xclaim");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let consumer_name = match arg_to_string(&args[2]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid consumer name"),
    };
    let min_idle = match arg_to_i64(&args[3]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut entry_ids = Vec::new();
    let mut idle: Option<u64> = None;
    let mut time: Option<u64> = None;
    let mut retrycount: Option<u64> = None;
    let mut force = false;
    let mut justid = false;

    let mut idx = 4;
    while idx < args.len() {
        if let Some(opt) = arg_to_string(&args[idx]) {
            let upper = opt.to_uppercase();
            match upper.as_str() {
                "IDLE" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xclaim");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n >= 0 => idle = Some(n as u64),
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    }
                }
                "TIME" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xclaim");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n >= 0 => time = Some(n as u64),
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    }
                }
                "RETRYCOUNT" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xclaim");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n >= 0 => retrycount = Some(n as u64),
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    }
                }
                "FORCE" => force = true,
                "JUSTID" => justid = true,
                _ => {
                    // Must be an entry ID
                    match StreamEntryId::parse(&opt) {
                        Some(id) => entry_ids.push(id),
                        None => {
                            return RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            );
                        }
                    }
                }
            }
        } else {
            return RespValue::error("ERR invalid argument");
        }
        idx += 1;
    }

    if entry_ids.is_empty() {
        return wrong_arg_count("xclaim");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => {
                match stream.xclaim(
                    &group_name,
                    &consumer_name,
                    min_idle,
                    &entry_ids,
                    idle,
                    time,
                    retrycount,
                    force,
                    justid,
                ) {
                    Ok(claimed) => {
                        let result: Vec<RespValue> = claimed
                            .into_iter()
                            .map(|(id, entry)| {
                                if justid {
                                    RespValue::bulk_string(id.to_string())
                                } else {
                                    match entry {
                                        Some(fields) => entry_to_resp(&id, &fields),
                                        None => RespValue::null_array(),
                                    }
                                }
                            })
                            .collect();
                        RespValue::array(result)
                    }
                    Err(e) => RespValue::error(e),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR no such key"),
    }
}

/// XAUTOCLAIM key group consumer min-idle start [COUNT count] [JUSTID]
pub async fn cmd_xautoclaim(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 5 {
        return wrong_arg_count("xautoclaim");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };
    let consumer_name = match arg_to_string(&args[2]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid consumer name"),
    };
    let min_idle = match arg_to_i64(&args[3]) {
        Some(n) if n >= 0 => n as u64,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let start_str = match arg_to_string(&args[4]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid stream ID"),
    };

    let start = if start_str == "0" || start_str == "0-0" {
        StreamEntryId::new(0, 0)
    } else {
        match StreamEntryId::parse(&start_str) {
            Some(id) => id,
            None => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                );
            }
        }
    };

    let mut count: usize = 100; // default
    let mut justid = false;

    let mut idx = 5;
    while idx < args.len() {
        if let Some(opt) = arg_to_string(&args[idx]) {
            let upper = opt.to_uppercase();
            match upper.as_str() {
                "COUNT" => {
                    idx += 1;
                    if idx >= args.len() {
                        return wrong_arg_count("xautoclaim");
                    }
                    match arg_to_i64(&args[idx]) {
                        Some(n) if n > 0 => count = n as usize,
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    }
                }
                "JUSTID" => justid = true,
                _ => return RespValue::error("ERR syntax error"),
            }
        } else {
            return RespValue::error("ERR invalid argument");
        }
        idx += 1;
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            RedisValue::Stream(stream) => {
                match stream.xautoclaim(
                    &group_name,
                    &consumer_name,
                    min_idle,
                    &start,
                    count,
                    justid,
                ) {
                    Ok((next_cursor, claimed, deleted)) => {
                        let cursor_resp =
                            RespValue::bulk_string(next_cursor.to_string().into_bytes());

                        let claimed_resp: Vec<RespValue> = claimed
                            .into_iter()
                            .map(|(id, entry)| {
                                if justid {
                                    RespValue::bulk_string(id.to_string())
                                } else {
                                    match entry {
                                        Some(fields) => entry_to_resp(&id, &fields),
                                        None => RespValue::null_array(),
                                    }
                                }
                            })
                            .collect();

                        let deleted_resp: Vec<RespValue> = deleted
                            .into_iter()
                            .map(|id| RespValue::bulk_string(id.to_string()))
                            .collect();

                        RespValue::array(vec![
                            cursor_resp,
                            RespValue::array(claimed_resp),
                            RespValue::array(deleted_resp),
                        ])
                    }
                    Err(e) => RespValue::error(e),
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR no such key"),
    }
}

/// XPENDING key group [[IDLE min-idle] start end count [consumer]]
pub async fn cmd_xpending(
    args: &[RespValue],
    store: &SharedStore,
    client: &ClientState,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("xpending");
    }

    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let group_name = match arg_to_string(&args[1]) {
        Some(g) => g,
        None => return RespValue::error("ERR invalid group name"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(stream) => {
                let group = match stream.get_group(&group_name) {
                    Some(g) => g,
                    None => {
                        return RespValue::error("NOGROUP No such consumer group for key");
                    }
                };

                if args.len() == 2 {
                    // Summary form
                    let (total, min_id, max_id, per_consumer) = group.pending_summary();
                    RespValue::array(vec![
                        RespValue::integer(total as i64),
                        match min_id {
                            Some(id) => RespValue::bulk_string(id.to_string()),
                            None => RespValue::null_bulk_string(),
                        },
                        match max_id {
                            Some(id) => RespValue::bulk_string(id.to_string()),
                            None => RespValue::null_bulk_string(),
                        },
                        if per_consumer.is_empty() {
                            RespValue::null_array()
                        } else {
                            RespValue::array(
                                per_consumer
                                    .into_iter()
                                    .map(|(name, count)| {
                                        RespValue::array(vec![
                                            RespValue::bulk_string(name),
                                            RespValue::bulk_string(count.to_string()),
                                        ])
                                    })
                                    .collect(),
                            )
                        },
                    ])
                } else {
                    // Detail form: XPENDING key group [[IDLE min-idle] start end count [consumer]]
                    let mut idx = 2;
                    let mut min_idle: u64 = 0;
                    let mut consumer_filter: Option<String> = None;

                    // Check for IDLE option
                    if let Some(opt) = arg_to_string(&args[idx])
                        && opt.eq_ignore_ascii_case("IDLE")
                    {
                        idx += 1;
                        if idx >= args.len() {
                            return wrong_arg_count("xpending");
                        }
                        match arg_to_i64(&args[idx]) {
                            Some(n) if n >= 0 => {
                                min_idle = n as u64;
                                idx += 1;
                            }
                            _ => {
                                return RespValue::error(
                                    "ERR value is not an integer or out of range",
                                );
                            }
                        }
                    }

                    if idx + 2 >= args.len() {
                        return wrong_arg_count("xpending");
                    }

                    let start_str = match arg_to_string(&args[idx]) {
                        Some(s) => s,
                        None => return RespValue::error("ERR invalid start ID"),
                    };
                    let end_str = match arg_to_string(&args[idx + 1]) {
                        Some(s) => s,
                        None => return RespValue::error("ERR invalid end ID"),
                    };
                    let count = match arg_to_i64(&args[idx + 2]) {
                        Some(n) if n >= 0 => n as usize,
                        _ => {
                            return RespValue::error("ERR value is not an integer or out of range");
                        }
                    };

                    idx += 3;

                    // Optional consumer filter
                    if idx < args.len() {
                        consumer_filter = arg_to_string(&args[idx]);
                    }

                    let start = match parse_range_bound(&start_str, true) {
                        Some(id) => id,
                        None => {
                            return RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            );
                        }
                    };

                    let end = match parse_range_bound(&end_str, false) {
                        Some(id) => id,
                        None => {
                            return RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            );
                        }
                    };

                    let entries = group.pending_range(
                        &start,
                        &end,
                        count,
                        consumer_filter.as_deref(),
                        min_idle,
                    );

                    let result: Vec<RespValue> = entries
                        .into_iter()
                        .map(|(id, consumer, idle, delivery_count)| {
                            RespValue::array(vec![
                                RespValue::bulk_string(id.to_string()),
                                RespValue::bulk_string(consumer),
                                RespValue::integer(idle as i64),
                                RespValue::integer(delivery_count as i64),
                            ])
                        })
                        .collect();

                    RespValue::array(result)
                }
            }
            _ => wrong_type_error(),
        },
        None => RespValue::error("ERR no such key"),
    }
}
