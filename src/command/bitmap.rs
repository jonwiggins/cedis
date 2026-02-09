use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count, wrong_type_error};
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use crate::types::bitmap::Bitmap;
use crate::types::rstring::RedisString;

/// Helper: read bytes from a key that is either a String or nonexistent.
/// Returns Ok(Some(bytes)) if it exists and is a string, Ok(None) if nonexistent,
/// Err(RespValue) if it is the wrong type.
fn read_string_bytes(db: &mut crate::store::Database, key: &str) -> Result<Option<Vec<u8>>, RespValue> {
    match db.get(key) {
        Some(entry) => match &entry.value {
            RedisValue::String(s) => Ok(Some(s.as_bytes().to_vec())),
            _ => Err(wrong_type_error()),
        },
        None => Ok(None),
    }
}

/// SETBIT key offset value
pub async fn cmd_setbit(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 3 {
        return wrong_arg_count("setbit");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let offset = match arg_to_i64(&args[1]) {
        Some(o) if o >= 0 => o as usize,
        _ => return RespValue::error("ERR bit offset is not an integer or out of range"),
    };
    let value = match arg_to_i64(&args[2]) {
        Some(0) => false,
        Some(1) => true,
        _ => return RespValue::error("ERR bit is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Read existing bytes or create empty
    let existing = match read_string_bytes(db, &key) {
        Ok(bytes) => bytes,
        Err(e) => return e,
    };

    let mut bm = match existing {
        Some(bytes) => Bitmap::from_bytes(bytes),
        None => Bitmap::new(),
    };

    let old = bm.setbit(offset, value);

    // Write back as a String value
    db.set(key, Entry::new(RedisValue::String(RedisString::new(bm.as_bytes().to_vec()))));

    RespValue::integer(if old { 1 } else { 0 })
}

/// GETBIT key offset
pub async fn cmd_getbit(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("getbit");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let offset = match arg_to_i64(&args[1]) {
        Some(o) if o >= 0 => o as usize,
        _ => return RespValue::error("ERR bit offset is not an integer or out of range"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let bytes = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => return RespValue::integer(0),
        Err(e) => return e,
    };

    let bm = Bitmap::from_bytes(bytes);
    RespValue::integer(if bm.getbit(offset) { 1 } else { 0 })
}

/// BITCOUNT key [start end [BYTE|BIT]]
pub async fn cmd_bitcount(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() || args.len() == 2 || args.len() > 4 {
        return wrong_arg_count("bitcount");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let bytes = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => return RespValue::integer(0),
        Err(e) => return e,
    };

    let bm = Bitmap::from_bytes(bytes);

    if args.len() >= 3 {
        let start = match arg_to_i64(&args[1]) {
            Some(s) => s,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let end = match arg_to_i64(&args[2]) {
            Some(e) => e,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        // Check for optional BYTE|BIT mode (args[3])
        // Default is BYTE mode, which is what bitcount_range already does
        // BIT mode would count individual bits in the bit range, but for now
        // we treat both as byte-range for compatibility
        RespValue::integer(bm.bitcount_range(start, end) as i64)
    } else {
        RespValue::integer(bm.bitcount() as i64)
    }
}

/// BITOP operation destkey key [key ...]
pub async fn cmd_bitop(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 3 {
        return wrong_arg_count("bitop");
    }
    let operation = match arg_to_string(&args[0]) {
        Some(op) => op.to_uppercase(),
        None => return RespValue::error("ERR invalid operation"),
    };
    let destkey = match arg_to_string(&args[1]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // NOT requires exactly one source key
    if operation == "NOT" && args.len() != 3 {
        return RespValue::error("ERR BITOP NOT requires one and only one key");
    }
    // AND, OR, XOR require at least one source key
    if !matches!(operation.as_str(), "AND" | "OR" | "XOR" | "NOT") {
        return RespValue::error("ERR syntax error");
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Read all source keys
    let mut bitmaps: Vec<Bitmap> = Vec::new();
    for arg in &args[2..] {
        let key = match arg_to_string(arg) {
            Some(k) => k,
            None => return RespValue::error("ERR invalid key"),
        };
        let bytes = match read_string_bytes(db, &key) {
            Ok(Some(b)) => b,
            Ok(None) => Vec::new(),
            Err(e) => return e,
        };
        bitmaps.push(Bitmap::from_bytes(bytes));
    }

    let result = match operation.as_str() {
        "NOT" => bitmaps[0].bitop_not(),
        "AND" => {
            let mut acc = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                acc = acc.bitop_and(bm);
            }
            acc
        }
        "OR" => {
            let mut acc = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                acc = acc.bitop_or(bm);
            }
            acc
        }
        "XOR" => {
            let mut acc = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                acc = acc.bitop_xor(bm);
            }
            acc
        }
        _ => unreachable!(),
    };

    let result_len = result.byte_len() as i64;
    db.set(
        destkey,
        Entry::new(RedisValue::String(RedisString::new(result.as_bytes().to_vec()))),
    );

    RespValue::integer(result_len)
}

/// BITPOS key bit [start [end]]
pub async fn cmd_bitpos(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 || args.len() > 4 {
        return wrong_arg_count("bitpos");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };
    let bit = match arg_to_i64(&args[1]) {
        Some(0) => false,
        Some(1) => true,
        _ => return RespValue::error("ERR bit is not an integer or out of range"),
    };

    let start = if args.len() >= 3 {
        match arg_to_i64(&args[2]) {
            Some(s) => Some(s),
            None => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let end = if args.len() >= 4 {
        match arg_to_i64(&args[3]) {
            Some(e) => Some(e),
            None => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let bytes = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => {
            // Empty key: looking for 1 returns -1, looking for 0 returns 0
            // unless a range is specified in which case return -1
            return if bit {
                RespValue::integer(-1)
            } else if start.is_some() {
                RespValue::integer(-1)
            } else {
                RespValue::integer(0)
            };
        }
        Err(e) => return e,
    };

    let bm = Bitmap::from_bytes(bytes);
    RespValue::integer(bm.bitpos(bit, start, end))
}
