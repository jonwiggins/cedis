pub mod string;
pub mod list;
pub mod hash;
pub mod set;
pub mod sorted_set;
pub mod key;
pub mod server_cmd;
pub mod pubsub;
pub mod transaction;

use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::pubsub::SharedPubSub;
use crate::resp::RespValue;
use crate::store::SharedStore;
use tokio::sync::mpsc;

/// Dispatch a parsed command to the appropriate handler.
pub async fn dispatch(
    cmd_name: &str,
    args: &[RespValue],
    store: &SharedStore,
    config: &SharedConfig,
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
) -> RespValue {
    // If in MULTI mode and this isn't EXEC/DISCARD/MULTI, queue the command
    if client.in_multi && !matches!(cmd_name, "EXEC" | "DISCARD" | "MULTI") {
        client.multi_queue.push((cmd_name.to_string(), args.to_vec()));
        return RespValue::SimpleString("QUEUED".to_string());
    }

    match cmd_name {
        // Connection
        "PING" => server_cmd::cmd_ping(args),
        "ECHO" => server_cmd::cmd_echo(args),
        "QUIT" => server_cmd::cmd_quit(client),
        "SELECT" => server_cmd::cmd_select(args, client, config).await,
        "AUTH" => server_cmd::cmd_auth(args, client, config).await,
        "DBSIZE" => server_cmd::cmd_dbsize(store, client).await,
        "FLUSHDB" => server_cmd::cmd_flushdb(store, client).await,
        "FLUSHALL" => server_cmd::cmd_flushall(store).await,
        "SWAPDB" => server_cmd::cmd_swapdb(args, store, config).await,

        // Server
        "INFO" => server_cmd::cmd_info(args, store, config).await,
        "CONFIG" => server_cmd::cmd_config(args, config).await,
        "TIME" => server_cmd::cmd_time(),
        "COMMAND" => server_cmd::cmd_command(args),
        "CLIENT" => server_cmd::cmd_client(args, client),
        "DEBUG" => server_cmd::cmd_debug(args).await,
        "RESET" => server_cmd::cmd_reset(client),

        // Strings
        "GET" => string::cmd_get(args, store, client).await,
        "SET" => string::cmd_set(args, store, client).await,
        "GETSET" => string::cmd_getset(args, store, client).await,
        "MGET" => string::cmd_mget(args, store, client).await,
        "MSET" => string::cmd_mset(args, store, client).await,
        "MSETNX" => string::cmd_msetnx(args, store, client).await,
        "APPEND" => string::cmd_append(args, store, client).await,
        "STRLEN" => string::cmd_strlen(args, store, client).await,
        "INCR" => string::cmd_incr(args, store, client).await,
        "DECR" => string::cmd_decr(args, store, client).await,
        "INCRBY" => string::cmd_incrby(args, store, client).await,
        "DECRBY" => string::cmd_decrby(args, store, client).await,
        "INCRBYFLOAT" => string::cmd_incrbyfloat(args, store, client).await,
        "SETNX" => string::cmd_setnx(args, store, client).await,
        "SETEX" => string::cmd_setex(args, store, client).await,
        "PSETEX" => string::cmd_psetex(args, store, client).await,
        "GETRANGE" => string::cmd_getrange(args, store, client).await,
        "SETRANGE" => string::cmd_setrange(args, store, client).await,
        "GETDEL" => string::cmd_getdel(args, store, client).await,

        // Keys
        "DEL" => key::cmd_del(args, store, client).await,
        "UNLINK" => key::cmd_del(args, store, client).await,
        "EXISTS" => key::cmd_exists(args, store, client).await,
        "EXPIRE" => key::cmd_expire(args, store, client).await,
        "PEXPIRE" => key::cmd_pexpire(args, store, client).await,
        "EXPIREAT" => key::cmd_expireat(args, store, client).await,
        "PEXPIREAT" => key::cmd_pexpireat(args, store, client).await,
        "TTL" => key::cmd_ttl(args, store, client).await,
        "PTTL" => key::cmd_pttl(args, store, client).await,
        "PERSIST" => key::cmd_persist(args, store, client).await,
        "TYPE" => key::cmd_type(args, store, client).await,
        "RENAME" => key::cmd_rename(args, store, client).await,
        "RENAMENX" => key::cmd_renamenx(args, store, client).await,
        "KEYS" => key::cmd_keys(args, store, client).await,
        "SCAN" => key::cmd_scan(args, store, client).await,
        "RANDOMKEY" => key::cmd_randomkey(store, client).await,
        "OBJECT" => key::cmd_object(args, store, client).await,
        "DUMP" => key::cmd_dump(args),
        "RESTORE" => key::cmd_restore(args),
        "SORT" => key::cmd_sort(args, store, client).await,

        // Lists
        "LPUSH" => list::cmd_lpush(args, store, client).await,
        "RPUSH" => list::cmd_rpush(args, store, client).await,
        "LPOP" => list::cmd_lpop(args, store, client).await,
        "RPOP" => list::cmd_rpop(args, store, client).await,
        "LLEN" => list::cmd_llen(args, store, client).await,
        "LRANGE" => list::cmd_lrange(args, store, client).await,
        "LINDEX" => list::cmd_lindex(args, store, client).await,
        "LSET" => list::cmd_lset(args, store, client).await,
        "LINSERT" => list::cmd_linsert(args, store, client).await,
        "LREM" => list::cmd_lrem(args, store, client).await,
        "LTRIM" => list::cmd_ltrim(args, store, client).await,
        "RPOPLPUSH" => list::cmd_rpoplpush(args, store, client).await,
        "LMOVE" => list::cmd_lmove(args, store, client).await,
        "LPOS" => list::cmd_lpos(args, store, client).await,
        "LMPOP" => list::cmd_lmpop(args, store, client).await,
        "BLPOP" => list::cmd_blpop(args, store, client).await,
        "BRPOP" => list::cmd_brpop(args, store, client).await,

        // Hashes
        "HSET" => hash::cmd_hset(args, store, client).await,
        "HGET" => hash::cmd_hget(args, store, client).await,
        "HDEL" => hash::cmd_hdel(args, store, client).await,
        "HEXISTS" => hash::cmd_hexists(args, store, client).await,
        "HLEN" => hash::cmd_hlen(args, store, client).await,
        "HKEYS" => hash::cmd_hkeys(args, store, client).await,
        "HVALS" => hash::cmd_hvals(args, store, client).await,
        "HGETALL" => hash::cmd_hgetall(args, store, client).await,
        "HMSET" => hash::cmd_hset(args, store, client).await,
        "HMGET" => hash::cmd_hmget(args, store, client).await,
        "HINCRBY" => hash::cmd_hincrby(args, store, client).await,
        "HINCRBYFLOAT" => hash::cmd_hincrbyfloat(args, store, client).await,
        "HSETNX" => hash::cmd_hsetnx(args, store, client).await,
        "HRANDFIELD" => hash::cmd_hrandfield(args, store, client).await,
        "HSCAN" => hash::cmd_hscan(args, store, client).await,

        // Sets
        "SADD" => set::cmd_sadd(args, store, client).await,
        "SREM" => set::cmd_srem(args, store, client).await,
        "SISMEMBER" => set::cmd_sismember(args, store, client).await,
        "SMISMEMBER" => set::cmd_smismember(args, store, client).await,
        "SMEMBERS" => set::cmd_smembers(args, store, client).await,
        "SCARD" => set::cmd_scard(args, store, client).await,
        "SPOP" => set::cmd_spop(args, store, client).await,
        "SRANDMEMBER" => set::cmd_srandmember(args, store, client).await,
        "SUNION" => set::cmd_sunion(args, store, client).await,
        "SINTER" => set::cmd_sinter(args, store, client).await,
        "SDIFF" => set::cmd_sdiff(args, store, client).await,
        "SUNIONSTORE" => set::cmd_sunionstore(args, store, client).await,
        "SINTERSTORE" => set::cmd_sinterstore(args, store, client).await,
        "SDIFFSTORE" => set::cmd_sdiffstore(args, store, client).await,
        "SMOVE" => set::cmd_smove(args, store, client).await,
        "SSCAN" => set::cmd_sscan(args, store, client).await,
        "SINTERCARD" => set::cmd_sintercard(args, store, client).await,

        // Sorted sets
        "ZADD" => sorted_set::cmd_zadd(args, store, client).await,
        "ZREM" => sorted_set::cmd_zrem(args, store, client).await,
        "ZSCORE" => sorted_set::cmd_zscore(args, store, client).await,
        "ZRANK" => sorted_set::cmd_zrank(args, store, client).await,
        "ZREVRANK" => sorted_set::cmd_zrevrank(args, store, client).await,
        "ZCARD" => sorted_set::cmd_zcard(args, store, client).await,
        "ZCOUNT" => sorted_set::cmd_zcount(args, store, client).await,
        "ZRANGE" => sorted_set::cmd_zrange(args, store, client).await,
        "ZREVRANGE" => sorted_set::cmd_zrevrange(args, store, client).await,
        "ZRANGEBYSCORE" => sorted_set::cmd_zrangebyscore(args, store, client).await,
        "ZREVRANGEBYSCORE" => sorted_set::cmd_zrevrangebyscore(args, store, client).await,
        "ZRANGEBYLEX" => sorted_set::cmd_zrangebylex(args, store, client).await,
        "ZREVRANGEBYLEX" => sorted_set::cmd_zrevrangebylex(args, store, client).await,
        "ZINCRBY" => sorted_set::cmd_zincrby(args, store, client).await,
        "ZUNIONSTORE" => sorted_set::cmd_zunionstore(args, store, client).await,
        "ZINTERSTORE" => sorted_set::cmd_zinterstore(args, store, client).await,
        "ZRANDMEMBER" => sorted_set::cmd_zrandmember(args, store, client).await,
        "ZSCAN" => sorted_set::cmd_zscan(args, store, client).await,
        "ZPOPMIN" => sorted_set::cmd_zpopmin(args, store, client).await,
        "ZPOPMAX" => sorted_set::cmd_zpopmax(args, store, client).await,
        "ZMSCORE" => sorted_set::cmd_zmscore(args, store, client).await,
        "ZLEXCOUNT" => sorted_set::cmd_zlexcount(args, store, client).await,

        // Transactions
        "MULTI" => transaction::cmd_multi(client),
        "EXEC" => transaction::cmd_exec(store, config, client, pubsub, pubsub_tx).await,
        "DISCARD" => transaction::cmd_discard(client),
        "WATCH" => transaction::cmd_watch(args, store, client).await,
        "UNWATCH" => transaction::cmd_unwatch(client),

        // Pub/Sub
        "SUBSCRIBE" => pubsub::cmd_subscribe(args, client, pubsub, pubsub_tx).await,
        "UNSUBSCRIBE" => pubsub::cmd_unsubscribe(args, client, pubsub, pubsub_tx).await,
        "PSUBSCRIBE" => pubsub::cmd_psubscribe(args, client, pubsub, pubsub_tx).await,
        "PUNSUBSCRIBE" => pubsub::cmd_punsubscribe(args, client, pubsub, pubsub_tx).await,
        "PUBLISH" => pubsub::cmd_publish(args, pubsub).await,
        "PUBSUB" => pubsub::cmd_pubsub(args, pubsub).await,

        // Persistence
        "SAVE" => server_cmd::cmd_save(store, config).await,
        "BGSAVE" => server_cmd::cmd_bgsave(store, config).await,
        "BGREWRITEAOF" => server_cmd::cmd_bgrewriteaof(store, config).await,
        "LASTSAVE" => server_cmd::cmd_lastsave(),

        _ => {
            let args_preview: Vec<String> = args
                .iter()
                .take(3)
                .filter_map(|a| a.to_string_lossy())
                .map(|s| format!("'{s}'"))
                .collect();
            RespValue::error(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                cmd_name,
                args_preview.join(" ")
            ))
        }
    }
}

/// Extract string bytes from a RespValue argument.
pub fn arg_to_bytes(arg: &RespValue) -> Option<&[u8]> {
    arg.as_str()
}

/// Extract a UTF-8 string from a RespValue argument.
pub fn arg_to_string(arg: &RespValue) -> Option<String> {
    arg.to_string_lossy()
}

/// Extract an i64 from a RespValue argument.
pub fn arg_to_i64(arg: &RespValue) -> Option<i64> {
    let s = arg.to_string_lossy()?;
    s.parse().ok()
}

/// Extract an f64 from a RespValue argument.
pub fn arg_to_f64(arg: &RespValue) -> Option<f64> {
    let s = arg.to_string_lossy()?;
    s.parse().ok()
}

/// Return a WRONGTYPE error.
pub fn wrong_type_error() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Return a wrong number of arguments error.
pub fn wrong_arg_count(cmd: &str) -> RespValue {
    RespValue::error(format!("ERR wrong number of arguments for '{cmd}' command"))
}
