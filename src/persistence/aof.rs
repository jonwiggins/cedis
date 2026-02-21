use crate::resp::RespValue;
use crate::store::DataStore;
use crate::types::RedisValue;
use std::io::{self, BufRead, Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

/// AOF writer that logs write commands.
pub struct AofWriter {
    file: Option<std::fs::File>,
    fsync_policy: FsyncPolicy,
}

#[derive(Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    Always,
    Everysec,
    No,
}

impl FsyncPolicy {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "always" => FsyncPolicy::Always,
            "everysec" => FsyncPolicy::Everysec,
            _ => FsyncPolicy::No,
        }
    }
}

impl Default for AofWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl AofWriter {
    pub fn new() -> Self {
        AofWriter {
            file: None,
            fsync_policy: FsyncPolicy::Everysec,
        }
    }

    /// Open or create the AOF file.
    pub fn open(&mut self, path: &str, policy: FsyncPolicy) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        self.file = Some(file);
        self.fsync_policy = policy;
        Ok(())
    }

    /// Log a write command.
    pub fn log_command(&mut self, cmd_name: &str, args: &[RespValue]) -> io::Result<()> {
        let file = match &mut self.file {
            Some(f) => f,
            None => return Ok(()),
        };

        // Build RESP array: [cmd_name, args...]
        let mut items = Vec::with_capacity(1 + args.len());
        items.push(RespValue::bulk_string(cmd_name.as_bytes().to_vec()));
        for arg in args {
            items.push(arg.clone());
        }
        let resp = RespValue::array(items);
        file.write_all(&resp.serialize())?;

        if self.fsync_policy == FsyncPolicy::Always {
            file.flush()?;
        }

        Ok(())
    }

    /// Flush the file to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(f) = &mut self.file {
            f.flush()?;
        }
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.file.is_some()
    }

    pub fn close(&mut self) {
        if let Some(f) = self.file.take() {
            let _ = f.sync_all();
        }
    }
}

/// Replay an AOF file to restore state.
pub fn replay(path: &str, store: &mut DataStore, num_databases: usize) -> io::Result<usize> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut reader = io::BufReader::new(file);
    let mut cmd_count = 0usize;
    let mut current_db = 0usize;

    loop {
        // Read one RESP value
        let value = match read_resp_value(&mut reader) {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(_) => break, // Truncated AOF, stop here
        };

        let items = match value {
            RespValue::Array(Some(items)) if !items.is_empty() => items,
            _ => continue,
        };

        let cmd_name = match items[0].to_string_lossy() {
            Some(s) => s.to_uppercase(),
            None => continue,
        };

        let args = &items[1..];

        // Apply the command to the store directly
        apply_command(store, &cmd_name, args, &mut current_db, num_databases);
        cmd_count += 1;
    }

    Ok(cmd_count)
}

/// Rewrite the AOF by scanning the current store state.
pub fn rewrite(store: &DataStore, path: &str) -> io::Result<()> {
    let tmp_path = format!("{path}.tmp");
    let mut file = std::fs::File::create(&tmp_path)?;

    for (db_index, db) in store.databases.iter().enumerate() {
        let entries: Vec<_> = db.iter().collect();
        if entries.is_empty() {
            continue;
        }

        // SELECT db
        let select_cmd = RespValue::array(vec![
            RespValue::bulk_string(b"SELECT".to_vec()),
            RespValue::bulk_string(db_index.to_string().into_bytes()),
        ]);
        file.write_all(&select_cmd.serialize())?;

        for (key, entry) in &entries {
            match &entry.value {
                RedisValue::String(s) => {
                    let cmd = RespValue::array(vec![
                        RespValue::bulk_string(b"SET".to_vec()),
                        RespValue::bulk_string(key.as_bytes().to_vec()),
                        RespValue::bulk_string(s.as_bytes().to_vec()),
                    ]);
                    file.write_all(&cmd.serialize())?;
                }
                RedisValue::List(list) => {
                    let items: Vec<_> = list.iter().collect();
                    if !items.is_empty() {
                        let mut cmd_parts = vec![
                            RespValue::bulk_string(b"RPUSH".to_vec()),
                            RespValue::bulk_string(key.as_bytes().to_vec()),
                        ];
                        for item in items {
                            cmd_parts.push(RespValue::bulk_string(item.to_vec()));
                        }
                        file.write_all(&RespValue::array(cmd_parts).serialize())?;
                    }
                }
                RedisValue::Hash(hash) => {
                    let fields: Vec<_> = hash.iter().collect();
                    if !fields.is_empty() {
                        let mut cmd_parts = vec![
                            RespValue::bulk_string(b"HSET".to_vec()),
                            RespValue::bulk_string(key.as_bytes().to_vec()),
                        ];
                        for (field, value) in fields {
                            cmd_parts.push(RespValue::bulk_string(field.as_bytes().to_vec()));
                            cmd_parts.push(RespValue::bulk_string(value.to_vec()));
                        }
                        file.write_all(&RespValue::array(cmd_parts).serialize())?;
                    }
                }
                RedisValue::Set(set) => {
                    let members = set.members();
                    if !members.is_empty() {
                        let mut cmd_parts = vec![
                            RespValue::bulk_string(b"SADD".to_vec()),
                            RespValue::bulk_string(key.as_bytes().to_vec()),
                        ];
                        for member in members {
                            cmd_parts.push(RespValue::bulk_string(member.to_vec()));
                        }
                        file.write_all(&RespValue::array(cmd_parts).serialize())?;
                    }
                }
                RedisValue::SortedSet(zset) => {
                    let items: Vec<_> = zset.iter().collect();
                    if !items.is_empty() {
                        let mut cmd_parts = vec![
                            RespValue::bulk_string(b"ZADD".to_vec()),
                            RespValue::bulk_string(key.as_bytes().to_vec()),
                        ];
                        for (member, score) in items {
                            cmd_parts.push(RespValue::bulk_string(score.to_string().into_bytes()));
                            cmd_parts.push(RespValue::bulk_string(member.to_vec()));
                        }
                        file.write_all(&RespValue::array(cmd_parts).serialize())?;
                    }
                }
                RedisValue::Stream(_) => {
                    // Stream AOF serialization not yet implemented; skip
                }
                RedisValue::HyperLogLog(_) => {
                    // HyperLogLog AOF serialization not yet implemented; skip
                }
                RedisValue::Geo(_) => {
                    // Geo AOF serialization not yet implemented; skip
                }
            }

            // Expiry
            if let Some(exp) = entry.expires_at {
                let cmd = RespValue::array(vec![
                    RespValue::bulk_string(b"PEXPIREAT".to_vec()),
                    RespValue::bulk_string(key.as_bytes().to_vec()),
                    RespValue::bulk_string(exp.to_string().into_bytes()),
                ]);
                file.write_all(&cmd.serialize())?;
            }
        }
    }

    file.flush()?;
    drop(file);
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Apply a single command to the store (for AOF replay).
fn apply_command(
    store: &mut DataStore,
    cmd: &str,
    args: &[RespValue],
    current_db: &mut usize,
    num_databases: usize,
) {
    let arg_str = |i: usize| -> Option<String> { args.get(i)?.to_string_lossy() };
    let arg_bytes = |i: usize| -> Option<Vec<u8>> { args.get(i)?.as_str().map(|b| b.to_vec()) };

    match cmd {
        "SELECT" => {
            if let Some(db) = arg_str(0).and_then(|s| s.parse::<usize>().ok())
                && db < num_databases
            {
                *current_db = db;
            }
        }
        "SET" => {
            if let (Some(key), Some(val)) = (arg_str(0), arg_bytes(1)) {
                let db = store.db(*current_db);
                let entry = crate::store::entry::Entry::new(RedisValue::String(
                    crate::types::rstring::RedisString::new(val),
                ));
                db.set(key, entry);
            }
        }
        "DEL" | "UNLINK" => {
            for arg in args {
                if let Some(key) = arg.to_string_lossy() {
                    store.db(*current_db).del(&key);
                }
            }
        }
        "RPUSH" | "LPUSH" => {
            if let Some(key) = arg_str(0) {
                let db = store.db(*current_db);
                if db.get(&key).is_none() {
                    db.set(
                        key.clone(),
                        crate::store::entry::Entry::new(RedisValue::List(
                            crate::types::list::RedisList::new(),
                        )),
                    );
                }
                if let Some(entry) = db.get_mut(&key)
                    && let RedisValue::List(list) = &mut entry.value
                {
                    for arg in &args[1..] {
                        if let Some(v) = arg.as_str() {
                            if cmd == "RPUSH" {
                                list.rpush(v.to_vec());
                            } else {
                                list.lpush(v.to_vec());
                            }
                        }
                    }
                }
            }
        }
        "HSET" => {
            if let Some(key) = arg_str(0) {
                let db = store.db(*current_db);
                if db.get(&key).is_none() {
                    db.set(
                        key.clone(),
                        crate::store::entry::Entry::new(RedisValue::Hash(
                            crate::types::hash::RedisHash::new(),
                        )),
                    );
                }
                if let Some(entry) = db.get_mut(&key)
                    && let RedisValue::Hash(hash) = &mut entry.value
                {
                    for pair in args[1..].chunks(2) {
                        if let (Some(field), Some(val)) = (
                            pair[0].to_string_lossy(),
                            pair.get(1).and_then(|v| v.as_str()),
                        ) {
                            hash.set(field, val.to_vec());
                        }
                    }
                }
            }
        }
        "SADD" => {
            if let Some(key) = arg_str(0) {
                let db = store.db(*current_db);
                if db.get(&key).is_none() {
                    db.set(
                        key.clone(),
                        crate::store::entry::Entry::new(RedisValue::Set(
                            crate::types::set::RedisSet::new(),
                        )),
                    );
                }
                if let Some(entry) = db.get_mut(&key)
                    && let RedisValue::Set(set) = &mut entry.value
                {
                    for arg in &args[1..] {
                        if let Some(member) = arg.as_str() {
                            set.add(member.to_vec());
                        }
                    }
                }
            }
        }
        "ZADD" => {
            if let Some(key) = arg_str(0) {
                let db = store.db(*current_db);
                if db.get(&key).is_none() {
                    db.set(
                        key.clone(),
                        crate::store::entry::Entry::new(RedisValue::SortedSet(
                            crate::types::sorted_set::RedisSortedSet::new(),
                        )),
                    );
                }
                if let Some(entry) = db.get_mut(&key)
                    && let RedisValue::SortedSet(zset) = &mut entry.value
                {
                    for pair in args[1..].chunks(2) {
                        if let (Some(score_str), Some(member)) = (
                            pair[0].to_string_lossy(),
                            pair.get(1).and_then(|v| v.as_str()),
                        ) && let Ok(score) = score_str.parse::<f64>()
                        {
                            zset.add(member.to_vec(), score);
                        }
                    }
                }
            }
        }
        "PEXPIREAT" => {
            if let (Some(key), Some(ts_str)) = (arg_str(0), arg_str(1))
                && let Ok(ts) = ts_str.parse::<u64>()
            {
                store.db(*current_db).set_expiry(&key, ts);
            }
        }
        "EXPIRE" => {
            if let (Some(key), Some(secs_str)) = (arg_str(0), arg_str(1))
                && let Ok(secs) = secs_str.parse::<u64>()
            {
                let ms = crate::store::entry::now_millis() + secs * 1000;
                store.db(*current_db).set_expiry(&key, ms);
            }
        }
        _ => {} // Skip unknown commands during replay
    }
}

/// Read a single RESP value from a buffered reader.
fn read_resp_value(reader: &mut io::BufReader<std::fs::File>) -> io::Result<Option<RespValue>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

    if line.is_empty() {
        return Ok(None);
    }

    let first = line.as_bytes()[0];
    let rest = &line[1..];

    match first {
        b'+' => Ok(Some(RespValue::SimpleString(rest.to_string()))),
        b'-' => Ok(Some(RespValue::Error(rest.to_string()))),
        b':' => {
            let n: i64 = rest
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad integer"))?;
            Ok(Some(RespValue::Integer(n)))
        }
        b'$' => {
            let len: i64 = rest
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad bulk len"))?;
            if len == -1 {
                return Ok(Some(RespValue::null_bulk_string()));
            }
            let len = len as usize;
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            reader.read_exact(&mut buf)?;
            buf.truncate(len);
            Ok(Some(RespValue::BulkString(Some(buf))))
        }
        b'*' => {
            let count: i64 = rest
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad array len"))?;
            if count == -1 {
                return Ok(Some(RespValue::null_array()));
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                match read_resp_value(reader)? {
                    Some(v) => items.push(v),
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated array",
                        ));
                    }
                }
            }
            Ok(Some(RespValue::Array(Some(items))))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown RESP byte: {first}"),
        )),
    }
}

pub type SharedAofWriter = Arc<Mutex<AofWriter>>;
