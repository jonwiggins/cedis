use crate::store::DataStore;
use crate::store::entry::Entry;
use crate::types::RedisValue;
use std::io::{self, Read, Write};

// RDB opcodes
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;

// RDB type bytes
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;

const RDB_MAGIC: &[u8] = b"REDIS";
const RDB_VERSION: &[u8] = b"0011";

/// Write the data store to an RDB file.
pub fn save(store: &DataStore, path: &str) -> io::Result<()> {
    let tmp_path = format!("{path}.tmp");
    let mut file = std::fs::File::create(&tmp_path)?;

    // Magic + version
    file.write_all(RDB_MAGIC)?;
    file.write_all(RDB_VERSION)?;

    for (db_index, db) in store.databases.iter().enumerate() {
        let entries: Vec<_> = db.iter().collect();
        if entries.is_empty() {
            continue;
        }

        // SELECTDB
        file.write_all(&[RDB_OPCODE_SELECTDB])?;
        write_length(&mut file, db_index as u64)?;

        // RESIZEDB
        let total = entries.len();
        let expires = entries
            .iter()
            .filter(|(_, e)| e.expires_at.is_some())
            .count();
        file.write_all(&[RDB_OPCODE_RESIZEDB])?;
        write_length(&mut file, total as u64)?;
        write_length(&mut file, expires as u64)?;

        for (key, entry) in &entries {
            // Expiry
            if let Some(exp) = entry.expires_at {
                file.write_all(&[RDB_OPCODE_EXPIRETIME_MS])?;
                file.write_all(&exp.to_le_bytes())?;
            }

            // Type byte + key + value
            match &entry.value {
                RedisValue::String(s) => {
                    file.write_all(&[RDB_TYPE_STRING])?;
                    write_string(&mut file, key.as_bytes())?;
                    write_string(&mut file, s.as_bytes())?;
                }
                RedisValue::List(list) => {
                    file.write_all(&[RDB_TYPE_LIST])?;
                    write_string(&mut file, key.as_bytes())?;
                    let items: Vec<_> = list.iter().collect();
                    write_length(&mut file, items.len() as u64)?;
                    for item in items {
                        write_string(&mut file, item)?;
                    }
                }
                RedisValue::Set(set) => {
                    file.write_all(&[RDB_TYPE_SET])?;
                    write_string(&mut file, key.as_bytes())?;
                    let members = set.members();
                    write_length(&mut file, members.len() as u64)?;
                    for member in members {
                        write_string(&mut file, member)?;
                    }
                }
                RedisValue::SortedSet(zset) => {
                    file.write_all(&[RDB_TYPE_ZSET])?;
                    write_string(&mut file, key.as_bytes())?;
                    let items: Vec<_> = zset.iter().collect();
                    write_length(&mut file, items.len() as u64)?;
                    for (member, score) in items {
                        write_string(&mut file, member)?;
                        file.write_all(&score.to_le_bytes())?;
                    }
                }
                RedisValue::Hash(hash) => {
                    file.write_all(&[RDB_TYPE_HASH])?;
                    write_string(&mut file, key.as_bytes())?;
                    let fields: Vec<_> = hash.iter().collect();
                    write_length(&mut file, fields.len() as u64)?;
                    for (field, value) in fields {
                        write_string(&mut file, field.as_bytes())?;
                        write_string(&mut file, value)?;
                    }
                }
                RedisValue::Stream(_) => {
                    // Stream RDB serialization not yet implemented; skip
                }
                RedisValue::HyperLogLog(_) => {
                    // HyperLogLog RDB serialization not yet implemented; skip
                }
                RedisValue::Geo(_) => {
                    // Geo RDB serialization not yet implemented; skip
                }
            }
        }
    }

    // EOF + 8 byte checksum (0 for now)
    file.write_all(&[RDB_OPCODE_EOF])?;
    file.write_all(&[0u8; 8])?;
    file.flush()?;
    drop(file);

    // Atomic rename
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Load an RDB file into a data store.
pub fn load(path: &str, num_databases: usize) -> io::Result<DataStore> {
    let mut file = std::fs::File::open(path)?;
    let mut store = DataStore::new(num_databases);

    // Read magic
    let mut magic = [0u8; 5];
    file.read_exact(&mut magic)?;
    if magic != *RDB_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid RDB magic",
        ));
    }

    // Read version
    let mut version = [0u8; 4];
    file.read_exact(&mut version)?;

    let mut current_db = 0usize;
    let mut next_expiry: Option<u64> = None;

    loop {
        let mut byte = [0u8; 1];
        if file.read_exact(&mut byte).is_err() {
            break;
        }

        match byte[0] {
            RDB_OPCODE_EOF => break,
            RDB_OPCODE_SELECTDB => {
                current_db = read_length(&mut file)? as usize;
                if current_db >= num_databases {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DB index out of range",
                    ));
                }
            }
            RDB_OPCODE_RESIZEDB => {
                let _db_size = read_length(&mut file)?;
                let _expires_size = read_length(&mut file)?;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let mut buf = [0u8; 8];
                file.read_exact(&mut buf)?;
                next_expiry = Some(u64::from_le_bytes(buf));
            }
            0xFD => {
                // EXPIRETIME in seconds
                let mut buf = [0u8; 4];
                file.read_exact(&mut buf)?;
                next_expiry = Some(u32::from_le_bytes(buf) as u64 * 1000);
            }
            type_byte => {
                let key = read_string_as_string(&mut file)?;
                let value = read_value(&mut file, type_byte)?;

                let mut entry = Entry::new(value);
                if let Some(exp) = next_expiry.take() {
                    entry.expires_at = Some(exp);
                }

                let db = store.db(current_db);
                db.set(key, entry);
            }
        }
    }

    Ok(store)
}

// --- Encoding helpers ---

fn write_length(w: &mut impl Write, len: u64) -> io::Result<()> {
    if len < 64 {
        w.write_all(&[len as u8])?;
    } else if len < 16384 {
        w.write_all(&[(0x40 | (len >> 8) as u8), len as u8])?;
    } else if len < (1 << 32) {
        w.write_all(&[0x80])?;
        w.write_all(&(len as u32).to_be_bytes())?;
    } else {
        w.write_all(&[0x81])?;
        w.write_all(&len.to_be_bytes())?;
    }
    Ok(())
}

fn write_string(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    write_length(w, data.len() as u64)?;
    w.write_all(data)?;
    Ok(())
}

fn read_length(r: &mut impl Read) -> io::Result<u64> {
    let mut byte = [0u8; 1];
    r.read_exact(&mut byte)?;
    let first = byte[0];

    match first >> 6 {
        0 => Ok((first & 0x3F) as u64),
        1 => {
            let mut next = [0u8; 1];
            r.read_exact(&mut next)?;
            Ok((((first & 0x3F) as u64) << 8) | next[0] as u64)
        }
        2 => {
            if first == 0x80 {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                Ok(u32::from_be_bytes(buf) as u64)
            } else {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok(u64::from_be_bytes(buf))
            }
        }
        3 => {
            // Special encoding: integer stored as string
            let enc_type = first & 0x3F;
            match enc_type {
                0 => {
                    let mut buf = [0u8; 1];
                    r.read_exact(&mut buf)?;
                    Ok(buf[0] as u64)
                }
                1 => {
                    let mut buf = [0u8; 2];
                    r.read_exact(&mut buf)?;
                    Ok(u16::from_le_bytes(buf) as u64)
                }
                2 => {
                    let mut buf = [0u8; 4];
                    r.read_exact(&mut buf)?;
                    Ok(u32::from_le_bytes(buf) as u64)
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unknown special encoding",
                )),
            }
        }
        _ => unreachable!(),
    }
}

fn read_string(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut byte = [0u8; 1];
    r.read_exact(&mut byte)?;
    let first = byte[0];

    if first >> 6 == 3 {
        // Special integer encoding - return as string bytes
        let enc_type = first & 0x3F;
        match enc_type {
            0 => {
                let mut buf = [0u8; 1];
                r.read_exact(&mut buf)?;
                Ok((buf[0] as i8).to_string().into_bytes())
            }
            1 => {
                let mut buf = [0u8; 2];
                r.read_exact(&mut buf)?;
                Ok((i16::from_le_bytes(buf)).to_string().into_bytes())
            }
            2 => {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                Ok((i32::from_le_bytes(buf)).to_string().into_bytes())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown special encoding",
            )),
        }
    } else {
        // Normal length-prefixed string
        let len = match first >> 6 {
            0 => (first & 0x3F) as u64,
            1 => {
                let mut next = [0u8; 1];
                r.read_exact(&mut next)?;
                (((first & 0x3F) as u64) << 8) | next[0] as u64
            }
            2 => {
                if first == 0x80 {
                    let mut buf = [0u8; 4];
                    r.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as u64
                } else {
                    let mut buf = [0u8; 8];
                    r.read_exact(&mut buf)?;
                    u64::from_be_bytes(buf)
                }
            }
            _ => unreachable!(),
        };
        let mut buf = vec![0u8; len as usize];
        r.read_exact(&mut buf)?;
        Ok(buf)
    }
}

fn read_string_as_string(r: &mut impl Read) -> io::Result<String> {
    let bytes = read_string(r)?;
    String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_value(r: &mut impl Read, type_byte: u8) -> io::Result<RedisValue> {
    match type_byte {
        RDB_TYPE_STRING => {
            let data = read_string(r)?;
            Ok(RedisValue::String(crate::types::rstring::RedisString::new(
                data,
            )))
        }
        RDB_TYPE_LIST => {
            let len = read_length(r)?;
            let mut list = crate::types::list::RedisList::new();
            for _ in 0..len {
                let item = read_string(r)?;
                list.rpush(item);
            }
            Ok(RedisValue::List(list))
        }
        RDB_TYPE_SET => {
            let len = read_length(r)?;
            let mut set = crate::types::set::RedisSet::new();
            for _ in 0..len {
                let member = read_string(r)?;
                set.add(member);
            }
            Ok(RedisValue::Set(set))
        }
        RDB_TYPE_ZSET => {
            let len = read_length(r)?;
            let mut zset = crate::types::sorted_set::RedisSortedSet::new();
            for _ in 0..len {
                let member = read_string(r)?;
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                let score = f64::from_le_bytes(buf);
                zset.add(member, score);
            }
            Ok(RedisValue::SortedSet(zset))
        }
        RDB_TYPE_HASH => {
            let len = read_length(r)?;
            let mut hash = crate::types::hash::RedisHash::new();
            for _ in 0..len {
                let field = read_string_as_string(r)?;
                let value = read_string(r)?;
                hash.set(field, value);
            }
            Ok(RedisValue::Hash(hash))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown RDB type byte: {type_byte}"),
        )),
    }
}
