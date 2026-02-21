pub mod bitmap;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod list;
pub mod rstring;
pub mod set;
pub mod sorted_set;
pub mod stream;

/// The core value type stored in the data store.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(rstring::RedisString),
    List(list::RedisList),
    Hash(hash::RedisHash),
    Set(set::RedisSet),
    SortedSet(sorted_set::RedisSortedSet),
    Stream(stream::RedisStream),
    HyperLogLog(hyperloglog::HyperLogLog),
    Geo(geo::GeoSet),
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
            RedisValue::Set(_) => "set",
            RedisValue::SortedSet(_) => "zset",
            RedisValue::Stream(_) => "stream",
            RedisValue::HyperLogLog(_) => "hyperloglog",
            RedisValue::Geo(_) => "zset",
        }
    }

    pub fn as_string(&self) -> Option<&rstring::RedisString> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_string_mut(&mut self) -> Option<&mut rstring::RedisString> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&list::RedisList> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut list::RedisList> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_hash(&self) -> Option<&hash::RedisHash> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut hash::RedisHash> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&set::RedisSet> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut set::RedisSet> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_sorted_set(&self) -> Option<&sorted_set::RedisSortedSet> {
        match self {
            RedisValue::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    pub fn as_sorted_set_mut(&mut self) -> Option<&mut sorted_set::RedisSortedSet> {
        match self {
            RedisValue::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    pub fn as_stream(&self) -> Option<&stream::RedisStream> {
        match self {
            RedisValue::Stream(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut stream::RedisStream> {
        match self {
            RedisValue::Stream(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_hyperloglog(&self) -> Option<&hyperloglog::HyperLogLog> {
        match self {
            RedisValue::HyperLogLog(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_hyperloglog_mut(&mut self) -> Option<&mut hyperloglog::HyperLogLog> {
        match self {
            RedisValue::HyperLogLog(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_geo(&self) -> Option<&geo::GeoSet> {
        match self {
            RedisValue::Geo(g) => Some(g),
            _ => None,
        }
    }

    pub fn as_geo_mut(&mut self) -> Option<&mut geo::GeoSet> {
        match self {
            RedisValue::Geo(g) => Some(g),
            _ => None,
        }
    }
}
