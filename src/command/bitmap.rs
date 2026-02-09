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

    let is_new_key = existing.is_none();
    let mut bm = match existing {
        Some(bytes) => Bitmap::from_bytes(bytes),
        None => Bitmap::new(),
    };

    let old_len = bm.byte_len();
    let old = bm.setbit(offset, value);
    let new_len = bm.byte_len();

    let is_dirty = old != value || old_len != new_len || is_new_key;

    // Write back as a String value
    db.set(key, Entry::new(RedisValue::String(RedisString::new(bm.as_bytes().to_vec()))));

    // Track dirty: increment when value changed, length changed, or key is new
    if is_dirty {
        store.dirty += 1;
    }

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
    if args.is_empty() {
        return wrong_arg_count("bitcount");
    }
    if args.len() == 2 || args.len() > 4 {
        return RespValue::error("ERR syntax error");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Validate arguments BEFORE accessing the store (so non-integer errors
    // take priority over WRONGTYPE errors, matching Redis behavior).
    let range = if args.len() >= 3 {
        let start = match arg_to_i64(&args[1]) {
            Some(s) => s,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let end = match arg_to_i64(&args[2]) {
            Some(e) => e,
            None => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let bit_mode = if args.len() == 4 {
            match arg_to_string(&args[3]) {
                Some(s) if s.eq_ignore_ascii_case("BIT") => true,
                Some(s) if s.eq_ignore_ascii_case("BYTE") => false,
                _ => return RespValue::error("ERR syntax error"),
            }
        } else {
            false
        };
        Some((start, end, bit_mode))
    } else {
        None
    };

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let bytes = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => return RespValue::integer(0),
        Err(e) => return e,
    };

    let bm = Bitmap::from_bytes(bytes);

    if let Some((start, end, bit_mode)) = range {
        if bit_mode {
            RespValue::integer(bm.bitcount_bit_range(start, end) as i64)
        } else {
            RespValue::integer(bm.bitcount_range(start, end) as i64)
        }
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
    // DIFF, DIFF1, ANDOR require at least two source keys
    if matches!(operation.as_str(), "DIFF" | "DIFF1" | "ANDOR") && args.len() < 4 {
        return RespValue::error(&format!("ERR BITOP {} requires at least two keys", operation));
    }
    if !matches!(operation.as_str(), "AND" | "OR" | "XOR" | "NOT" | "ONE" | "DIFF" | "DIFF1" | "ANDOR") {
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
        "ONE" => Bitmap::bitop_one(&bitmaps),
        "DIFF" => Bitmap::bitop_diff(&bitmaps),
        "DIFF1" => Bitmap::bitop_diff1(&bitmaps),
        "ANDOR" => Bitmap::bitop_andor(&bitmaps),
        _ => unreachable!(),
    };

    let result_len = result.byte_len() as i64;
    db.set(
        destkey,
        Entry::new(RedisValue::String(RedisString::new(result.as_bytes().to_vec()))),
    );

    RespValue::integer(result_len)
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
pub async fn cmd_bitpos(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.len() < 2 || args.len() > 5 {
        return wrong_arg_count("bitpos");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Validate mode argument FIRST (before parsing integers) so that
    // BITPOS key bit start end GARBAGE returns "ERR syntax error"
    let bit_mode = if args.len() == 5 {
        match arg_to_string(&args[4]) {
            Some(s) if s.eq_ignore_ascii_case("BIT") => true,
            Some(s) if s.eq_ignore_ascii_case("BYTE") => false,
            _ => return RespValue::error("ERR syntax error"),
        }
    } else {
        false
    };

    let bit = match arg_to_i64(&args[1]) {
        Some(0) => false,
        Some(1) => true,
        Some(_) => return RespValue::error("ERR bit is not an integer or out of range"),
        None => return RespValue::error("ERR value is not an integer or out of range"),
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
            return if bit {
                RespValue::integer(-1)
            } else {
                // BYTE mode with explicit end: -1 (Redis "changes behavior if end is given")
                // BIT mode or no end specified: 0 (conceptually infinite string of 0s)
                RespValue::integer(if !bit_mode && end.is_some() { -1 } else { 0 })
            };
        }
        Err(e) => return e,
    };

    let bm = Bitmap::from_bytes(bytes);
    if bit_mode {
        RespValue::integer(bm.bitpos_bit(bit, start.unwrap_or(0), end.unwrap_or(-1)))
    } else {
        RespValue::integer(bm.bitpos(bit, start, end))
    }
}

/// Parse a BITFIELD type encoding like "i5", "u8", "i32", etc.
/// Returns (signed, bits) or None on error.
fn parse_bitfield_type(s: &str) -> Option<(bool, u32)> {
    let s = s.to_lowercase();
    let (signed, rest) = if s.starts_with('i') {
        (true, &s[1..])
    } else if s.starts_with('u') {
        (false, &s[1..])
    } else {
        return None;
    };
    let bits: u32 = rest.parse().ok()?;
    if bits == 0 || bits > 64 || (!signed && bits > 63) {
        return None;
    }
    Some((signed, bits))
}

/// Parse a BITFIELD offset like "0", "10", "#5" (# means multiply by type bits).
fn parse_bitfield_offset(s: &str, type_bits: u32) -> Option<u64> {
    if let Some(rest) = s.strip_prefix('#') {
        let idx: u64 = rest.parse().ok()?;
        Some(idx * type_bits as u64)
    } else {
        s.parse().ok()
    }
}

/// Read `bits` bits starting at `bit_offset` from a byte slice, as an unsigned u64.
fn read_bits(data: &[u8], bit_offset: u64, bits: u32) -> u64 {
    let mut value: u64 = 0;
    for i in 0..bits as u64 {
        let pos = bit_offset + i;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = 7 - (pos % 8) as u32;
        if byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1 {
            value |= 1u64 << (bits as u64 - 1 - i);
        }
    }
    value
}

/// Write `bits` bits starting at `bit_offset` to a byte vec, from an unsigned u64.
/// Grows the vec if needed. Returns true if the value changed.
fn write_bits(data: &mut Vec<u8>, bit_offset: u64, bits: u32, value: u64) -> bool {
    let last_bit = bit_offset + bits as u64 - 1;
    let last_byte = (last_bit / 8) as usize;
    if last_byte >= data.len() {
        data.resize(last_byte + 1, 0);
    }
    let mut changed = false;
    for i in 0..bits as u64 {
        let pos = bit_offset + i;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = 7 - (pos % 8) as u32;
        let new_bit = ((value >> (bits as u64 - 1 - i)) & 1) == 1;
        let old_bit = (data[byte_idx] >> bit_idx) & 1 == 1;
        if new_bit != old_bit {
            changed = true;
            if new_bit {
                data[byte_idx] |= 1 << bit_idx;
            } else {
                data[byte_idx] &= !(1 << bit_idx);
            }
        }
    }
    changed
}

/// Convert raw bits to signed value.
fn to_signed(value: u64, bits: u32) -> i64 {
    if bits == 64 {
        value as i64
    } else if value & (1u64 << (bits - 1)) != 0 {
        // Sign extend
        (value | !((1u64 << bits) - 1)) as i64
    } else {
        value as i64
    }
}

/// Wrap a value to fit in the given number of bits (signed or unsigned).
fn wrap_value(value: i64, signed: bool, bits: u32) -> u64 {
    if bits == 64 {
        value as u64
    } else if signed {
        let mask = (1u64 << bits) - 1;
        (value as u64) & mask
    } else {
        let mask = (1u64 << bits) - 1;
        (value as u64) & mask
    }
}

/// Saturate a value to fit in the given number of bits.
fn saturate_value(value: i64, signed: bool, bits: u32) -> u64 {
    if signed {
        let min = if bits == 64 { i64::MIN } else { -(1i64 << (bits - 1)) };
        let max = if bits == 64 { i64::MAX } else { (1i64 << (bits - 1)) - 1 };
        let clamped = value.max(min).min(max);
        wrap_value(clamped, true, bits)
    } else {
        let max = if bits >= 64 { u64::MAX } else { (1u64 << bits) - 1 };
        let clamped = if value < 0 { 0u64 } else { (value as u64).min(max) };
        clamped
    }
}

/// BITFIELD key [GET type offset | SET type offset value | INCRBY type offset increment | OVERFLOW WRAP|SAT|FAIL] ...
pub async fn cmd_bitfield(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("bitfield");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    // Parse operations
    enum Op {
        Get { signed: bool, bits: u32, offset: u64 },
        Set { signed: bool, bits: u32, offset: u64, value: i64 },
        IncrBy { signed: bool, bits: u32, offset: u64, increment: i64 },
    }

    #[derive(Clone, Copy)]
    enum Overflow { Wrap, Sat, Fail }

    let mut ops: Vec<(Op, Overflow)> = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 1;
    let mut has_writes = false;

    while i < args.len() {
        let subcmd = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR invalid bitfield subcommand"),
        };
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return RespValue::error("ERR invalid bitfield command syntax");
                }
                let type_str = match arg_to_string(&args[i + 1]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield type"),
                };
                let (signed, bits) = match parse_bitfield_type(&type_str) {
                    Some(t) => t,
                    None => return RespValue::error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is"),
                };
                let offset_str = match arg_to_string(&args[i + 2]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield offset"),
                };
                let offset = match parse_bitfield_offset(&offset_str, bits) {
                    Some(o) => o,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                ops.push((Op::Get { signed, bits, offset }, overflow));
                i += 3;
            }
            "SET" => {
                if i + 3 >= args.len() {
                    return RespValue::error("ERR invalid bitfield command syntax");
                }
                let type_str = match arg_to_string(&args[i + 1]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield type"),
                };
                let (signed, bits) = match parse_bitfield_type(&type_str) {
                    Some(t) => t,
                    None => return RespValue::error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is"),
                };
                let offset_str = match arg_to_string(&args[i + 2]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield offset"),
                };
                let offset = match parse_bitfield_offset(&offset_str, bits) {
                    Some(o) => o,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                let value = match arg_to_i64(&args[i + 3]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR value is not an integer or out of range"),
                };
                ops.push((Op::Set { signed, bits, offset, value }, overflow));
                has_writes = true;
                i += 4;
            }
            "INCRBY" => {
                if i + 3 >= args.len() {
                    return RespValue::error("ERR invalid bitfield command syntax");
                }
                let type_str = match arg_to_string(&args[i + 1]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield type"),
                };
                let (signed, bits) = match parse_bitfield_type(&type_str) {
                    Some(t) => t,
                    None => return RespValue::error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is"),
                };
                let offset_str = match arg_to_string(&args[i + 2]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield offset"),
                };
                let offset = match parse_bitfield_offset(&offset_str, bits) {
                    Some(o) => o,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                let increment = match arg_to_i64(&args[i + 3]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR value is not an integer or out of range"),
                };
                ops.push((Op::IncrBy { signed, bits, offset, increment }, overflow));
                has_writes = true;
                i += 4;
            }
            "OVERFLOW" => {
                if i + 1 >= args.len() {
                    return RespValue::error("ERR invalid bitfield command syntax");
                }
                let mode = match arg_to_string(&args[i + 1]) {
                    Some(s) => s.to_uppercase(),
                    None => return RespValue::error("ERR invalid overflow mode"),
                };
                overflow = match mode.as_str() {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return RespValue::error("ERR Invalid OVERFLOW type (should be one of WRAP, SAT, FAIL)"),
                };
                i += 2;
            }
            _ => return RespValue::error(format!("ERR Unknown bitfield subcommand '{subcmd}'")),
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    // Read existing bytes
    let mut data = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => Vec::new(),
        Err(e) => return e,
    };

    let key_existed = !data.is_empty();
    let mut results: Vec<RespValue> = Vec::new();
    let mut dirty = 0u64;

    for (op, ov) in &ops {
        match op {
            Op::Get { signed, bits, offset } => {
                let raw = read_bits(&data, *offset, *bits);
                let val = if *signed { to_signed(raw, *bits) } else { raw as i64 };
                results.push(RespValue::integer(val));
            }
            Op::Set { signed, bits, offset, value } => {
                let old_raw = read_bits(&data, *offset, *bits);
                let old_val = if *signed { to_signed(old_raw, *bits) } else { old_raw as i64 };
                let old_len = data.len();
                let new_raw = wrap_value(*value, *signed, *bits);
                let changed = write_bits(&mut data, *offset, *bits, new_raw);
                let len_changed = data.len() != old_len;
                if changed || len_changed || !key_existed {
                    dirty += 1;
                }
                results.push(RespValue::integer(old_val));
            }
            Op::IncrBy { signed, bits, offset, increment } => {
                let old_raw = read_bits(&data, *offset, *bits);
                let old_val = if *signed { to_signed(old_raw, *bits) } else { old_raw as i64 };
                let new_val = old_val.wrapping_add(*increment);

                match ov {
                    Overflow::Wrap => {
                        let wrapped = wrap_value(new_val, *signed, *bits);
                        write_bits(&mut data, *offset, *bits, wrapped);
                        let result_val = if *signed { to_signed(wrapped, *bits) } else { wrapped as i64 };
                        results.push(RespValue::integer(result_val));
                        dirty += 1;
                    }
                    Overflow::Sat => {
                        let sat = saturate_value(new_val, *signed, *bits);
                        write_bits(&mut data, *offset, *bits, sat);
                        let result_val = if *signed { to_signed(sat, *bits) } else { sat as i64 };
                        results.push(RespValue::integer(result_val));
                        dirty += 1;
                    }
                    Overflow::Fail => {
                        // Check if overflow would occur
                        let wrapped = wrap_value(new_val, *signed, *bits);
                        let result_val = if *signed { to_signed(wrapped, *bits) } else { wrapped as i64 };
                        if result_val != new_val {
                            // Overflow occurred, don't apply
                            results.push(RespValue::null_bulk_string());
                        } else {
                            write_bits(&mut data, *offset, *bits, wrapped);
                            results.push(RespValue::integer(result_val));
                            dirty += 1;
                        }
                    }
                }
            }
        }
    }

    // Write back if we had any write operations
    if has_writes {
        db.set(key, Entry::new(RedisValue::String(RedisString::new(data))));
    }

    // Track dirty count
    store.dirty += dirty;

    RespValue::Array(Some(results))
}

/// BITFIELD_RO key [GET type offset] ...
pub async fn cmd_bitfield_ro(args: &[RespValue], store: &SharedStore, client: &ClientState) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("bitfield_ro");
    }
    let key = match arg_to_string(&args[0]) {
        Some(k) => k,
        None => return RespValue::error("ERR invalid key"),
    };

    let mut results: Vec<RespValue> = Vec::new();
    let mut i = 1;

    // Parse and validate all GET operations first
    struct GetOp { signed: bool, bits: u32, offset: u64 }
    let mut ops: Vec<GetOp> = Vec::new();

    while i < args.len() {
        let subcmd = match arg_to_string(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return RespValue::error("ERR invalid bitfield subcommand"),
        };
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return RespValue::error("ERR invalid bitfield command syntax");
                }
                let type_str = match arg_to_string(&args[i + 1]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield type"),
                };
                let (signed, bits) = match parse_bitfield_type(&type_str) {
                    Some(t) => t,
                    None => return RespValue::error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is"),
                };
                let offset_str = match arg_to_string(&args[i + 2]) {
                    Some(s) => s,
                    None => return RespValue::error("ERR invalid bitfield offset"),
                };
                let offset = match parse_bitfield_offset(&offset_str, bits) {
                    Some(o) => o,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                ops.push(GetOp { signed, bits, offset });
                i += 3;
            }
            _ => return RespValue::error("ERR BITFIELD_RO only supports the GET subcommand"),
        }
    }

    let mut store = store.write().await;
    let db = store.db(client.db_index);

    let data = match read_string_bytes(db, &key) {
        Ok(Some(b)) => b,
        Ok(None) => Vec::new(),
        Err(e) => return e,
    };

    for op in &ops {
        let raw = read_bits(&data, op.offset, op.bits);
        let val = if op.signed { to_signed(raw, op.bits) } else { raw as i64 };
        results.push(RespValue::integer(val));
    }

    RespValue::Array(Some(results))
}
