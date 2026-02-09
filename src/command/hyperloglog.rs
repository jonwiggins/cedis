use crate::command::{arg_to_bytes, arg_to_string, wrong_arg_count};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::hyperloglog::HyperLogLog;

/// Helper: get or create a HyperLogLog at the given key.
/// Returns Err(RespValue) if the key holds the wrong type.
fn get_or_create_hll<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut HyperLogLog, RespValue> {
    if !db.exists(key) {
        db.set(
            key.to_string(),
            Entry::new(RedisValue::HyperLogLog(HyperLogLog::new())),
        );
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            RedisValue::HyperLogLog(hll) => Ok(hll),
            _ => Err(RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => unreachable!(),
    }
}

/// PFADD key element [element ...]
pub async fn cmd_pfadd(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("pfadd");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);
    let hll = match get_or_create_hll(db, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    // If no elements provided, just ensure the key exists and return 0
    if args.len() == 1 {
        return RespValue::integer(0);
    }

    let mut changed = false;
    for arg in &args[1..] {
        if let Some(element) = arg_to_bytes(arg) {
            if hll.add(element) {
                changed = true;
            }
        }
    }

    RespValue::integer(if changed { 1 } else { 0 })
}

/// PFCOUNT key [key ...]
pub async fn cmd_pfcount(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("pfcount");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    if args.len() == 1 {
        // Single key: return its count
        let key = match arg_to_string(&args[0]) {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::HyperLogLog(hll) => RespValue::integer(hll.count() as i64),
                _ => RespValue::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ),
            },
            None => RespValue::integer(0),
        }
    } else {
        // Multiple keys: merge into a temporary HLL and count
        let mut merged = HyperLogLog::new();
        for arg in args {
            let key = match arg_to_string(arg) {
                Some(k) => k,
                None => return RespValue::error("ERR invalid key"),
            };
            match db.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::HyperLogLog(hll) => {
                        merged.merge(hll);
                    }
                    _ => {
                        return RespValue::error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                },
                None => {
                    // Nonexistent key contributes nothing
                }
            }
        }
        RespValue::integer(merged.count() as i64)
    }
}

/// PFMERGE destkey sourcekey [sourcekey ...]
pub async fn cmd_pfmerge(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("pfmerge");
    }
    let destkey = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // First, collect all source HLLs into a temporary merged HLL
    let mut merged = HyperLogLog::new();

    // If destkey already exists as an HLL, include it in the merge
    match db.get(&destkey) {
        Some(entry) => match &entry.value {
            RedisValue::HyperLogLog(hll) => {
                merged.merge(hll);
            }
            _ => {
                return RespValue::error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
        },
        None => {}
    }

    // Merge all source keys
    for arg in &args[1..] {
        let key = match arg_to_string(arg) {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::HyperLogLog(hll) => {
                    merged.merge(hll);
                }
                _ => {
                    return RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            },
            None => {
                // Nonexistent key contributes nothing
            }
        }
    }

    // Store the merged result at destkey
    db.set(
        destkey,
        Entry::new(RedisValue::HyperLogLog(merged)),
    );

    RespValue::ok()
}
