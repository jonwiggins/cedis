//! Lua scripting engine for EVAL / EVALSHA support.
//!
//! Embeds Lua 5.4 via `mlua` and provides `redis.call()` / `redis.pcall()`
//! that execute commands directly against the data store while the Lua VM runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mlua::prelude::*;

use crate::resp::RespValue;
use crate::store::DataStore;

/// Compute the SHA1 hex digest of a script.
pub fn sha1_hex(script: &str) -> String {
    let hash = sha1_smol::Sha1::from(script).digest();
    hash.to_string()
}

/// Global script cache: SHA1 -> source.
#[derive(Debug, Clone)]
pub struct ScriptCache {
    scripts: Arc<Mutex<HashMap<String, String>>>,
}

impl ScriptCache {
    pub fn new() -> Self {
        ScriptCache {
            scripts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Insert a script and return its SHA1 hex digest.
    pub fn load(&self, script: &str) -> String {
        let sha = sha1_hex(script);
        let mut map = self.scripts.lock().unwrap();
        map.insert(sha.clone(), script.to_string());
        sha
    }

    /// Look up a script by SHA1.
    pub fn get(&self, sha: &str) -> Option<String> {
        let map = self.scripts.lock().unwrap();
        map.get(sha).cloned()
    }

    /// Check whether a SHA1 exists in the cache.
    pub fn exists(&self, sha: &str) -> bool {
        let map = self.scripts.lock().unwrap();
        map.contains_key(sha)
    }

    /// Flush all cached scripts.
    pub fn flush(&self) {
        let mut map = self.scripts.lock().unwrap();
        map.clear();
    }
}

// ---------------------------------------------------------------------------
// Conversion: RespValue <-> Lua
// ---------------------------------------------------------------------------

/// Convert a RespValue to a Lua value.
///
/// Redis -> Lua conversion rules (matching real Redis):
///   Integer  -> number
///   BulkString(Some) -> string
///   BulkString(None) -> false (Lua boolean)
///   SimpleString -> Lua table { ok = string }
///   Error -> Lua table { err = string }
///   Array(Some) -> Lua table (1-indexed sequence)
///   Array(None) -> false
fn resp_to_lua(lua: &Lua, val: &RespValue) -> LuaResult<LuaValue> {
    match val {
        RespValue::Integer(n) => Ok(LuaValue::Integer(*n)),
        RespValue::BulkString(Some(data)) => {
            let s = lua.create_string(data)?;
            Ok(LuaValue::String(s))
        }
        RespValue::BulkString(None) => Ok(LuaValue::Boolean(false)),
        RespValue::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", lua.create_string(s.as_bytes())?)?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Error(s) => {
            let t = lua.create_table()?;
            t.set("err", lua.create_string(s.as_bytes())?)?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Array(Some(items)) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::Array(None) => Ok(LuaValue::Boolean(false)),
    }
}

/// Convert a Lua value back to a RespValue.
///
/// Lua -> Redis conversion rules (matching real Redis):
///   number -> Integer (truncated)
///   string -> BulkString
///   boolean true -> Integer(1)
///   boolean false / nil -> BulkString(None)  (null)
///   table with .ok -> SimpleString
///   table with .err -> Error
///   table (array) -> Array
fn lua_to_resp(val: LuaValue) -> RespValue {
    match val {
        LuaValue::Integer(n) => RespValue::Integer(n),
        LuaValue::Number(n) => RespValue::Integer(n as i64),
        LuaValue::String(s) => RespValue::bulk_string(s.as_bytes().to_vec()),
        LuaValue::Boolean(true) => RespValue::Integer(1),
        LuaValue::Boolean(false) | LuaValue::Nil => RespValue::null_bulk_string(),
        LuaValue::Table(t) => {
            // Check for status tables: { ok = ... } or { err = ... }
            if let Ok(ok_val) = t.get::<LuaValue>("ok") {
                if let LuaValue::String(s) = ok_val {
                    let text = String::from_utf8_lossy(&s.as_bytes()).to_string();
                    return RespValue::SimpleString(text);
                }
            }
            if let Ok(err_val) = t.get::<LuaValue>("err") {
                if let LuaValue::String(s) = err_val {
                    let text = String::from_utf8_lossy(&s.as_bytes()).to_string();
                    return RespValue::Error(text);
                }
            }
            // Otherwise treat as an array table (1-indexed)
            let len = t.raw_len();
            let mut items = Vec::with_capacity(len);
            for i in 1..=len {
                let v: LuaValue = t.get(i).unwrap_or(LuaValue::Nil);
                items.push(lua_to_resp(v));
            }
            RespValue::Array(Some(items))
        }
        _ => RespValue::null_bulk_string(),
    }
}

// ---------------------------------------------------------------------------
// Synchronous command execution inside Lua
// ---------------------------------------------------------------------------

/// Execute a Redis command synchronously against the data store.
///
/// This is called from within the Lua VM for `redis.call()` / `redis.pcall()`.
/// The caller must already hold a write lock on the store (passed via raw pointer).
///
/// We only support a curated set of simple commands here. The store pointer is
/// valid because the Lua execution lives entirely within the scope of the
/// async function that holds the write guard.
fn execute_redis_command(store: &mut DataStore, db_index: usize, args: &[String]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR wrong number of arguments for 'redis.call'");
    }

    let cmd = args[0].to_uppercase();
    let cmd_args: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();

    match cmd.as_str() {
        // -- Strings ---------------------------------------------------------
        "GET" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'get' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::String(s) => {
                        RespValue::bulk_string(s.as_bytes().to_vec())
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::null_bulk_string(),
            }
        }

        "SET" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'set' command");
            }
            let key = cmd_args[0].to_string();
            let value = cmd_args[1].as_bytes().to_vec();
            let db = store.db(db_index);
            let entry = crate::store::entry::Entry::new(crate::types::RedisValue::String(
                crate::types::rstring::RedisString::new(value),
            ));
            db.set(key, entry);
            RespValue::ok()
        }

        "SETNX" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'setnx' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            if db.exists(key) {
                RespValue::integer(0)
            } else {
                let entry = crate::store::entry::Entry::new(crate::types::RedisValue::String(
                    crate::types::rstring::RedisString::new(cmd_args[1].as_bytes().to_vec()),
                ));
                db.set(key.to_string(), entry);
                RespValue::integer(1)
            }
        }

        "MGET" => {
            if cmd_args.is_empty() {
                return RespValue::error("ERR wrong number of arguments for 'mget' command");
            }
            let db = store.db(db_index);
            let mut results = Vec::with_capacity(cmd_args.len());
            for key in &cmd_args {
                match db.get(key) {
                    Some(entry) => match &entry.value {
                        crate::types::RedisValue::String(s) => {
                            results.push(RespValue::bulk_string(s.as_bytes().to_vec()));
                        }
                        _ => results.push(RespValue::null_bulk_string()),
                    },
                    None => results.push(RespValue::null_bulk_string()),
                }
            }
            RespValue::array(results)
        }

        "MSET" => {
            if cmd_args.len() < 2 || cmd_args.len() % 2 != 0 {
                return RespValue::error("ERR wrong number of arguments for 'mset' command");
            }
            let db = store.db(db_index);
            for pair in cmd_args.chunks(2) {
                let entry = crate::store::entry::Entry::new(crate::types::RedisValue::String(
                    crate::types::rstring::RedisString::new(pair[1].as_bytes().to_vec()),
                ));
                db.set(pair[0].to_string(), entry);
            }
            RespValue::ok()
        }

        "APPEND" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'append' command");
            }
            let key = cmd_args[0];
            let data = cmd_args[1].as_bytes();
            let db = store.db(db_index);
            if !db.exists(key) {
                let entry = crate::store::entry::Entry::new(crate::types::RedisValue::String(
                    crate::types::rstring::RedisString::new(data.to_vec()),
                ));
                db.set(key.to_string(), entry);
                return RespValue::integer(data.len() as i64);
            }
            match db.get_mut(key) {
                Some(entry) => match &mut entry.value {
                    crate::types::RedisValue::String(s) => {
                        let new_len = s.append(data);
                        RespValue::integer(new_len as i64)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => unreachable!(),
            }
        }

        "STRLEN" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'strlen' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::String(s) => RespValue::integer(s.len() as i64),
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "INCR" => script_incr(store, db_index, cmd_args[0], 1),
        "DECR" => script_incr(store, db_index, cmd_args[0], -1),
        "INCRBY" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'incrby' command");
            }
            match cmd_args[1].parse::<i64>() {
                Ok(delta) => script_incr(store, db_index, cmd_args[0], delta),
                Err(_) => RespValue::error("ERR value is not an integer or out of range"),
            }
        }
        "DECRBY" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'decrby' command");
            }
            match cmd_args[1].parse::<i64>() {
                Ok(delta) => script_incr(store, db_index, cmd_args[0], -delta),
                Err(_) => RespValue::error("ERR value is not an integer or out of range"),
            }
        }

        // -- Keys ------------------------------------------------------------
        "DEL" | "UNLINK" => {
            if cmd_args.is_empty() {
                return RespValue::error("ERR wrong number of arguments for 'del' command");
            }
            let db = store.db(db_index);
            let mut count = 0i64;
            for key in &cmd_args {
                if db.del(key) {
                    count += 1;
                }
            }
            RespValue::integer(count)
        }

        "EXISTS" => {
            if cmd_args.is_empty() {
                return RespValue::error("ERR wrong number of arguments for 'exists' command");
            }
            let db = store.db(db_index);
            let mut count = 0i64;
            for key in &cmd_args {
                if db.exists(key) {
                    count += 1;
                }
            }
            RespValue::integer(count)
        }

        "TYPE" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'type' command");
            }
            let db = store.db(db_index);
            let type_name = db.key_type(cmd_args[0]).unwrap_or("none");
            RespValue::SimpleString(type_name.to_string())
        }

        "TTL" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'ttl' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => RespValue::integer(entry.ttl_seconds()),
                None => RespValue::integer(-2),
            }
        }

        "PTTL" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'pttl' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => RespValue::integer(entry.ttl_millis()),
                None => RespValue::integer(-2),
            }
        }

        "EXPIRE" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'expire' command");
            }
            match cmd_args[1].parse::<i64>() {
                Ok(seconds) => {
                    if seconds <= 0 {
                        let db = store.db(db_index);
                        return RespValue::integer(if db.del(cmd_args[0]) { 1 } else { 0 });
                    }
                    let expires_at =
                        crate::store::entry::now_millis() + (seconds as u64) * 1000;
                    let db = store.db(db_index);
                    RespValue::integer(if db.set_expiry(cmd_args[0], expires_at) {
                        1
                    } else {
                        0
                    })
                }
                Err(_) => RespValue::error("ERR value is not an integer or out of range"),
            }
        }

        "PERSIST" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'persist' command");
            }
            let db = store.db(db_index);
            RespValue::integer(if db.persist(cmd_args[0]) { 1 } else { 0 })
        }

        "RENAME" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'rename' command");
            }
            let db = store.db(db_index);
            if db.rename(cmd_args[0], cmd_args[1]) {
                RespValue::ok()
            } else {
                RespValue::error("ERR no such key")
            }
        }

        "KEYS" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'keys' command");
            }
            let db = store.db(db_index);
            let keys = db.keys(cmd_args[0]);
            let items: Vec<RespValue> = keys.into_iter().map(|k| RespValue::bulk_string(k)).collect();
            RespValue::array(items)
        }

        // -- Hashes ----------------------------------------------------------
        "HSET" => {
            if cmd_args.len() < 3 || (cmd_args.len() - 1) % 2 != 0 {
                return RespValue::error("ERR wrong number of arguments for 'hset' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            ensure_hash(db, key);
            let hash = match get_hash_mut(db, key) {
                Ok(h) => h,
                Err(e) => return e,
            };
            let mut new_fields = 0i64;
            for pair in cmd_args[1..].chunks(2) {
                if hash.set(pair[0].to_string(), pair[1].as_bytes().to_vec()) {
                    new_fields += 1;
                }
            }
            RespValue::integer(new_fields)
        }

        "HGET" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'hget' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => match h.get(cmd_args[1]) {
                        Some(v) => RespValue::bulk_string(v.clone()),
                        None => RespValue::null_bulk_string(),
                    },
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::null_bulk_string(),
            }
        }

        "HDEL" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'hdel' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            match db.get_mut(key) {
                Some(entry) => match &mut entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        let mut count = 0i64;
                        for field in &cmd_args[1..] {
                            if h.del(field) {
                                count += 1;
                            }
                        }
                        RespValue::integer(count)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "HEXISTS" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'hexists' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        RespValue::integer(if h.get(cmd_args[1]).is_some() { 1 } else { 0 })
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "HLEN" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'hlen' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => RespValue::integer(h.len() as i64),
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "HGETALL" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'hgetall' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        let mut items = Vec::new();
                        for (field, value) in h.iter() {
                            items.push(RespValue::bulk_string(field.as_bytes().to_vec()));
                            items.push(RespValue::bulk_string(value.clone()));
                        }
                        RespValue::array(items)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::array(vec![]),
            }
        }

        "HMGET" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'hmget' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        let mut results = Vec::new();
                        for field in &cmd_args[1..] {
                            match h.get(field) {
                                Some(v) => results.push(RespValue::bulk_string(v.clone())),
                                None => results.push(RespValue::null_bulk_string()),
                            }
                        }
                        RespValue::array(results)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => {
                    let nulls = vec![RespValue::null_bulk_string(); cmd_args.len() - 1];
                    RespValue::array(nulls)
                }
            }
        }

        "HKEYS" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'hkeys' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        let items: Vec<RespValue> = h
                            .iter()
                            .map(|(f, _)| RespValue::bulk_string(f.as_bytes().to_vec()))
                            .collect();
                        RespValue::array(items)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::array(vec![]),
            }
        }

        "HVALS" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'hvals' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Hash(h) => {
                        let items: Vec<RespValue> =
                            h.iter().map(|(_, v)| RespValue::bulk_string(v.clone())).collect();
                        RespValue::array(items)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::array(vec![]),
            }
        }

        // -- Lists -----------------------------------------------------------
        "LPUSH" => script_list_push(store, db_index, &cmd_args, true),
        "RPUSH" => script_list_push(store, db_index, &cmd_args, false),
        "LPOP" => script_list_pop(store, db_index, &cmd_args, true),
        "RPOP" => script_list_pop(store, db_index, &cmd_args, false),

        "LLEN" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'llen' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::List(l) => RespValue::integer(l.len() as i64),
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "LRANGE" => {
            if cmd_args.len() != 3 {
                return RespValue::error("ERR wrong number of arguments for 'lrange' command");
            }
            let start = match cmd_args[1].parse::<i64>() {
                Ok(n) => n,
                Err(_) => {
                    return RespValue::error("ERR value is not an integer or out of range")
                }
            };
            let stop = match cmd_args[2].parse::<i64>() {
                Ok(n) => n,
                Err(_) => {
                    return RespValue::error("ERR value is not an integer or out of range")
                }
            };
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::List(l) => {
                        let items = l.lrange(start, stop);
                        let resp: Vec<RespValue> =
                            items.into_iter().map(|v| RespValue::bulk_string(v.clone())).collect();
                        RespValue::array(resp)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::array(vec![]),
            }
        }

        "LINDEX" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'lindex' command");
            }
            let index = match cmd_args[1].parse::<i64>() {
                Ok(n) => n,
                Err(_) => {
                    return RespValue::error("ERR value is not an integer or out of range")
                }
            };
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::List(l) => match l.lindex(index) {
                        Some(v) => RespValue::bulk_string(v.clone()),
                        None => RespValue::null_bulk_string(),
                    },
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::null_bulk_string(),
            }
        }

        // -- Sets ------------------------------------------------------------
        "SADD" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'sadd' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            ensure_set(db, key);
            let set = match get_set_mut(db, key) {
                Ok(s) => s,
                Err(e) => return e,
            };
            let mut count = 0i64;
            for member in &cmd_args[1..] {
                if set.add(member.as_bytes().to_vec()) {
                    count += 1;
                }
            }
            RespValue::integer(count)
        }

        "SREM" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'srem' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            match db.get_mut(key) {
                Some(entry) => match &mut entry.value {
                    crate::types::RedisValue::Set(s) => {
                        let mut count = 0i64;
                        for member in &cmd_args[1..] {
                            if s.remove(member.as_bytes()) {
                                count += 1;
                            }
                        }
                        RespValue::integer(count)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "SISMEMBER" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'sismember' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Set(s) => {
                        RespValue::integer(if s.contains(cmd_args[1].as_bytes()) { 1 } else { 0 })
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "SMEMBERS" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'smembers' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Set(s) => {
                        let items: Vec<RespValue> = s
                            .members()
                            .into_iter()
                            .map(|m| RespValue::bulk_string(m.clone()))
                            .collect();
                        RespValue::array(items)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::array(vec![]),
            }
        }

        "SCARD" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'scard' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::Set(s) => RespValue::integer(s.len() as i64),
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        // -- Sorted Sets (basic) ---------------------------------------------
        "ZADD" => {
            // ZADD key score member [score member ...]
            if cmd_args.len() < 3 || (cmd_args.len() - 1) % 2 != 0 {
                return RespValue::error("ERR wrong number of arguments for 'zadd' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            ensure_sorted_set(db, key);
            let zset = match get_sorted_set_mut(db, key) {
                Ok(z) => z,
                Err(e) => return e,
            };
            let mut count = 0i64;
            for pair in cmd_args[1..].chunks(2) {
                let score: f64 = match pair[0].parse() {
                    Ok(s) => s,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                if zset.add(pair[1].as_bytes().to_vec(), score) {
                    count += 1;
                }
            }
            RespValue::integer(count)
        }

        "ZSCORE" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'zscore' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::SortedSet(z) => match z.score(cmd_args[1].as_bytes()) {
                        Some(s) => RespValue::bulk_string(format!("{s}")),
                        None => RespValue::null_bulk_string(),
                    },
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::null_bulk_string(),
            }
        }

        "ZCARD" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'zcard' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::SortedSet(z) => RespValue::integer(z.len() as i64),
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "ZREM" => {
            if cmd_args.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'zrem' command");
            }
            let key = cmd_args[0];
            let db = store.db(db_index);
            match db.get_mut(key) {
                Some(entry) => match &mut entry.value {
                    crate::types::RedisValue::SortedSet(z) => {
                        let mut count = 0i64;
                        for member in &cmd_args[1..] {
                            if z.remove(member.as_bytes()) {
                                count += 1;
                            }
                        }
                        RespValue::integer(count)
                    }
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::integer(0),
            }
        }

        "ZRANK" => {
            if cmd_args.len() != 2 {
                return RespValue::error("ERR wrong number of arguments for 'zrank' command");
            }
            let db = store.db(db_index);
            match db.get(cmd_args[0]) {
                Some(entry) => match &entry.value {
                    crate::types::RedisValue::SortedSet(z) => match z.rank(cmd_args[1].as_bytes()) {
                        Some(r) => RespValue::integer(r as i64),
                        None => RespValue::null_bulk_string(),
                    },
                    _ => RespValue::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                },
                None => RespValue::null_bulk_string(),
            }
        }

        // -- Misc ------------------------------------------------------------
        "PING" => {
            if cmd_args.is_empty() {
                RespValue::SimpleString("PONG".to_string())
            } else {
                RespValue::bulk_string(cmd_args[0].as_bytes().to_vec())
            }
        }

        "ECHO" => {
            if cmd_args.len() != 1 {
                return RespValue::error("ERR wrong number of arguments for 'echo' command");
            }
            RespValue::bulk_string(cmd_args[0].as_bytes().to_vec())
        }

        "DBSIZE" => {
            let db = store.db(db_index);
            RespValue::integer(db.dbsize() as i64)
        }

        "FLUSHDB" => {
            let db = store.db(db_index);
            db.flush();
            RespValue::ok()
        }

        _ => RespValue::error(format!(
            "ERR unknown or unsupported command '{}' called from Lua script",
            cmd
        )),
    }
}

// ---------------------------------------------------------------------------
// Helper functions for the script command executor
// ---------------------------------------------------------------------------

fn script_incr(store: &mut DataStore, db_index: usize, key: &str, delta: i64) -> RespValue {
    let db = store.db(db_index);
    if !db.exists(key) {
        let entry = crate::store::entry::Entry::new(crate::types::RedisValue::String(
            crate::types::rstring::RedisString::from_str("0"),
        ));
        db.set(key.to_string(), entry);
    }
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::String(s) => match s.incr_by(delta) {
                Ok(n) => RespValue::integer(n),
                Err(e) => RespValue::error(format!("ERR {e}")),
            },
            _ => RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
        },
        None => unreachable!(),
    }
}

fn script_list_push(
    store: &mut DataStore,
    db_index: usize,
    cmd_args: &[&str],
    left: bool,
) -> RespValue {
    if cmd_args.len() < 2 {
        let name = if left { "lpush" } else { "rpush" };
        return RespValue::error(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    }
    let key = cmd_args[0];
    let db = store.db(db_index);
    ensure_list(db, key);
    let list = match get_list_mut(db, key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    for val in &cmd_args[1..] {
        if left {
            list.lpush(val.as_bytes().to_vec());
        } else {
            list.rpush(val.as_bytes().to_vec());
        }
    }
    RespValue::integer(list.len() as i64)
}

fn script_list_pop(
    store: &mut DataStore,
    db_index: usize,
    cmd_args: &[&str],
    left: bool,
) -> RespValue {
    if cmd_args.len() != 1 {
        let name = if left { "lpop" } else { "rpop" };
        return RespValue::error(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    }
    let db = store.db(db_index);
    match db.get_mut(cmd_args[0]) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::List(l) => {
                let val = if left { l.lpop() } else { l.rpop() };
                match val {
                    Some(v) => RespValue::bulk_string(v),
                    None => RespValue::null_bulk_string(),
                }
            }
            _ => RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
        },
        None => RespValue::null_bulk_string(),
    }
}

fn ensure_list(db: &mut crate::store::Database, key: &str) {
    if !db.exists(key) {
        let list = crate::types::list::RedisList::new();
        db.set(
            key.to_string(),
            crate::store::entry::Entry::new(crate::types::RedisValue::List(list)),
        );
    }
}

fn get_list_mut<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut crate::types::list::RedisList, RespValue> {
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::List(l) => Ok(l),
            _ => Err(RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Err(RespValue::error("ERR no such key")),
    }
}

fn ensure_hash(db: &mut crate::store::Database, key: &str) {
    if !db.exists(key) {
        let hash = crate::types::hash::RedisHash::new();
        db.set(
            key.to_string(),
            crate::store::entry::Entry::new(crate::types::RedisValue::Hash(hash)),
        );
    }
}

fn get_hash_mut<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut crate::types::hash::RedisHash, RespValue> {
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::Hash(h) => Ok(h),
            _ => Err(RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Err(RespValue::error("ERR no such key")),
    }
}

fn ensure_set(db: &mut crate::store::Database, key: &str) {
    if !db.exists(key) {
        let set = crate::types::set::RedisSet::new();
        db.set(
            key.to_string(),
            crate::store::entry::Entry::new(crate::types::RedisValue::Set(set)),
        );
    }
}

fn get_set_mut<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut crate::types::set::RedisSet, RespValue> {
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::Set(s) => Ok(s),
            _ => Err(RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Err(RespValue::error("ERR no such key")),
    }
}

fn ensure_sorted_set(db: &mut crate::store::Database, key: &str) {
    if !db.exists(key) {
        let zset = crate::types::sorted_set::RedisSortedSet::new();
        db.set(
            key.to_string(),
            crate::store::entry::Entry::new(crate::types::RedisValue::SortedSet(zset)),
        );
    }
}

fn get_sorted_set_mut<'a>(
    db: &'a mut crate::store::Database,
    key: &str,
) -> Result<&'a mut crate::types::sorted_set::RedisSortedSet, RespValue> {
    match db.get_mut(key) {
        Some(entry) => match &mut entry.value {
            crate::types::RedisValue::SortedSet(z) => Ok(z),
            _ => Err(RespValue::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Err(RespValue::error("ERR no such key")),
    }
}

// ---------------------------------------------------------------------------
// Public API: run a Lua script
// ---------------------------------------------------------------------------

/// Evaluate a Lua script with the given KEYS and ARGV arrays.
///
/// `store` must be a mutable reference to the DataStore (the caller already
/// holds the write lock).  This function creates a short-lived Lua VM,
/// registers the `redis.call()` / `redis.pcall()` globals, then runs the
/// script and converts the return value back to `RespValue`.
pub fn eval_script(
    script: &str,
    keys: &[String],
    argv: &[String],
    store: &mut DataStore,
    db_index: usize,
) -> RespValue {
    let lua = match Lua::new() {
        lua => lua,
    };

    // We pass a raw pointer to the DataStore into the Lua closures.
    // SAFETY: the pointer is valid for the entire duration of `lua.load(...).call()`,
    // because this function holds `&mut DataStore` (and hence the write guard)
    // for the entire call. The Lua VM is created and destroyed within this
    // function, so the pointer cannot escape.
    let store_ptr = store as *mut DataStore;

    // Set up KEYS and ARGV globals
    if let Err(e) = setup_globals(&lua, keys, argv, store_ptr, db_index) {
        return RespValue::error(format!("ERR setting up Lua globals: {e}"));
    }

    // Execute the script
    match lua.load(script).eval::<LuaValue>() {
        Ok(val) => lua_to_resp(val),
        Err(e) => {
            let msg = e.to_string();
            // If the error message already starts with a Redis-like prefix, keep it
            if msg.starts_with("ERR") || msg.starts_with("WRONGTYPE") || msg.starts_with("NOSCRIPT") {
                RespValue::error(msg)
            } else {
                RespValue::error(format!("ERR {msg}"))
            }
        }
    }
}

/// Set up KEYS, ARGV, and the `redis` table in the Lua environment.
fn setup_globals(
    lua: &Lua,
    keys: &[String],
    argv: &[String],
    store_ptr: *mut DataStore,
    db_index: usize,
) -> LuaResult<()> {
    // KEYS table (1-indexed)
    let keys_table = lua.create_table()?;
    for (i, key) in keys.iter().enumerate() {
        keys_table.set(i + 1, lua.create_string(key.as_bytes())?)?;
    }
    lua.globals().set("KEYS", keys_table)?;

    // ARGV table (1-indexed)
    let argv_table = lua.create_table()?;
    for (i, arg) in argv.iter().enumerate() {
        argv_table.set(i + 1, lua.create_string(arg.as_bytes())?)?;
    }
    lua.globals().set("ARGV", argv_table)?;

    // redis.call() — raises a Lua error on Redis errors
    let call_store_ptr = store_ptr as usize;
    let call_db = db_index;
    let redis_call = lua.create_function(move |lua, args: LuaMultiValue| {
        let store: &mut DataStore = unsafe { &mut *(call_store_ptr as *mut DataStore) };
        let str_args = lua_args_to_strings(&args)?;
        let result = execute_redis_command(store, call_db, &str_args);
        // redis.call() raises on errors
        if let RespValue::Error(ref msg) = result {
            return Err(LuaError::RuntimeError(msg.clone()));
        }
        resp_to_lua(lua, &result)
    })?;

    // redis.pcall() — returns errors as tables instead of raising
    let pcall_store_ptr = store_ptr as usize;
    let pcall_db = db_index;
    let redis_pcall = lua.create_function(move |lua, args: LuaMultiValue| {
        let store: &mut DataStore = unsafe { &mut *(pcall_store_ptr as *mut DataStore) };
        let str_args = lua_args_to_strings(&args)?;
        let result = execute_redis_command(store, pcall_db, &str_args);
        // redis.pcall() never raises — errors are returned as tables
        resp_to_lua(lua, &result)
    })?;

    // redis.error_reply() helper
    let redis_error_reply = lua.create_function(|lua, msg: LuaString| {
        let t = lua.create_table()?;
        t.set("err", msg)?;
        Ok(LuaValue::Table(t))
    })?;

    // redis.status_reply() helper
    let redis_status_reply = lua.create_function(|lua, msg: LuaString| {
        let t = lua.create_table()?;
        t.set("ok", msg)?;
        Ok(LuaValue::Table(t))
    })?;

    // redis.log() — no-op for now (just discards output)
    let redis_log = lua.create_function(|_lua, _args: LuaMultiValue| -> LuaResult<()> {
        Ok(())
    })?;

    // Build the `redis` table
    let redis_table = lua.create_table()?;
    redis_table.set("call", redis_call)?;
    redis_table.set("pcall", redis_pcall)?;
    redis_table.set("error_reply", redis_error_reply)?;
    redis_table.set("status_reply", redis_status_reply)?;
    redis_table.set("log", redis_log)?;

    // Log-level constants
    redis_table.set("LOG_DEBUG", 0)?;
    redis_table.set("LOG_VERBOSE", 1)?;
    redis_table.set("LOG_NOTICE", 2)?;
    redis_table.set("LOG_WARNING", 3)?;

    lua.globals().set("redis", redis_table)?;

    Ok(())
}

/// Convert Lua multi-value arguments to a Vec<String>.
fn lua_args_to_strings(args: &LuaMultiValue) -> LuaResult<Vec<String>> {
    let mut result = Vec::with_capacity(args.len());
    for val in args {
        match val {
            LuaValue::String(s) => {
                result.push(String::from_utf8_lossy(&s.as_bytes()).to_string());
            }
            LuaValue::Integer(n) => {
                result.push(n.to_string());
            }
            LuaValue::Number(n) => {
                result.push(format!("{n}"));
            }
            LuaValue::Boolean(b) => {
                result.push(if *b { "1".to_string() } else { "0".to_string() });
            }
            _ => {
                return Err(LuaError::RuntimeError(
                    "ERR Lua redis.call() accepts only strings and numbers as arguments".to_string(),
                ));
            }
        }
    }
    Ok(result)
}
