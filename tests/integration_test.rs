use redis::Commands;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};

fn start_server(port: u16) -> tokio::task::JoinHandle<()> {
    let config = cedis::config::Config {
        port,
        ..Default::default()
    };
    let num_dbs = config.databases;
    let config = Arc::new(RwLock::new(config));
    let store = Arc::new(RwLock::new(cedis::store::DataStore::new(num_dbs)));
    let pubsub = Arc::new(RwLock::new(cedis::pubsub::PubSubRegistry::new()));
    let aof = Arc::new(Mutex::new(cedis::persistence::aof::AofWriter::new()));

    tokio::spawn(async move {
        let _ = cedis::server::run_server(store, config, pubsub, aof).await;
    })
}

fn get_client(port: u16) -> redis::Connection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    // Retry connection a few times
    for i in 0..50 {
        match client.get_connection() {
            Ok(conn) => return conn,
            Err(_) if i < 49 => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => panic!("Failed to connect: {e}"),
        }
    }
    unreachable!()
}

#[tokio::test]
async fn test_ping() {
    let port = 16379;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let result: String = redis::cmd("PING").query(&mut conn).unwrap();
        assert_eq!(result, "PONG");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_echo() {
    let port = 16380;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let result: String = redis::cmd("ECHO").arg("hello world").query(&mut conn).unwrap();
        assert_eq!(result, "hello world");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_set_get() {
    let port = 16381;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "myvalue").unwrap();
        let val: String = conn.get("mykey").unwrap();
        assert_eq!(val, "myvalue");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_incr_decr() {
    let port = 16382;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("counter", "10").unwrap();
        let val: i64 = conn.incr("counter", 1).unwrap();
        assert_eq!(val, 11);

        let val: i64 = conn.incr("counter", 5).unwrap();
        assert_eq!(val, 16);

        let val: i64 = conn.decr("counter", 3).unwrap();
        assert_eq!(val, 13);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_del_exists() {
    let port = 16383;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("key1", "val1").unwrap();
        let _: () = conn.set("key2", "val2").unwrap();

        let exists: bool = conn.exists("key1").unwrap();
        assert!(exists);

        let deleted: i64 = conn.del("key1").unwrap();
        assert_eq!(deleted, 1);

        let exists: bool = conn.exists("key1").unwrap();
        assert!(!exists);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_expire_ttl() {
    let port = 16384;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "myvalue").unwrap();
        let _: () = conn.expire("mykey", 100).unwrap();

        let ttl: i64 = conn.ttl("mykey").unwrap();
        assert!(ttl > 0 && ttl <= 100);

        // Key without TTL
        let _: () = conn.set("noexpiry", "value").unwrap();
        let ttl: i64 = conn.ttl("noexpiry").unwrap();
        assert_eq!(ttl, -1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_mset_mget() {
    let port = 16385;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = redis::cmd("MSET")
            .arg("k1").arg("v1")
            .arg("k2").arg("v2")
            .arg("k3").arg("v3")
            .query(&mut conn)
            .unwrap();

        let vals: Vec<String> = redis::cmd("MGET")
            .arg("k1").arg("k2").arg("k3")
            .query(&mut conn)
            .unwrap();
        assert_eq!(vals, vec!["v1", "v2", "v3"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_list_operations() {
    let port = 16386;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.lpush("mylist", "c").unwrap();
        let _: () = conn.lpush("mylist", "b").unwrap();
        let _: () = conn.lpush("mylist", "a").unwrap();
        let _: () = conn.rpush("mylist", "d").unwrap();

        let len: i64 = conn.llen("mylist").unwrap();
        assert_eq!(len, 4);

        let range: Vec<String> = conn.lrange("mylist", 0, -1).unwrap();
        assert_eq!(range, vec!["a", "b", "c", "d"]);

        let val: String = conn.lpop("mylist", None).unwrap();
        assert_eq!(val, "a");

        let val: String = conn.rpop("mylist", None).unwrap();
        assert_eq!(val, "d");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_hash_operations() {
    let port = 16387;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.hset("myhash", "field1", "value1").unwrap();
        let _: () = conn.hset("myhash", "field2", "value2").unwrap();

        let val: String = conn.hget("myhash", "field1").unwrap();
        assert_eq!(val, "value1");

        let len: i64 = conn.hlen("myhash").unwrap();
        assert_eq!(len, 2);

        let exists: bool = conn.hexists("myhash", "field1").unwrap();
        assert!(exists);

        let _: () = conn.hdel("myhash", "field1").unwrap();
        let exists: bool = conn.hexists("myhash", "field1").unwrap();
        assert!(!exists);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_set_operations() {
    let port = 16388;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.sadd("myset", "a").unwrap();
        let _: () = conn.sadd("myset", "b").unwrap();
        let _: () = conn.sadd("myset", "c").unwrap();
        let _: () = conn.sadd("myset", "a").unwrap(); // duplicate

        let card: i64 = conn.scard("myset").unwrap();
        assert_eq!(card, 3);

        let is_member: bool = conn.sismember("myset", "a").unwrap();
        assert!(is_member);

        let is_member: bool = conn.sismember("myset", "z").unwrap();
        assert!(!is_member);

        let _: () = conn.srem("myset", "a").unwrap();
        let card: i64 = conn.scard("myset").unwrap();
        assert_eq!(card, 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sorted_set_operations() {
    let port = 16389;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.zadd("leaderboard", "alice", 100).unwrap();
        let _: () = conn.zadd("leaderboard", "bob", 200).unwrap();
        let _: () = conn.zadd("leaderboard", "charlie", 150).unwrap();

        let card: i64 = conn.zcard("leaderboard").unwrap();
        assert_eq!(card, 3);

        let score: f64 = conn.zscore("leaderboard", "bob").unwrap();
        assert_eq!(score, 200.0);

        let rank: i64 = conn.zrank("leaderboard", "alice").unwrap();
        assert_eq!(rank, 0); // lowest score = rank 0

        // Top players (descending)
        let top: Vec<String> = redis::cmd("ZREVRANGE")
            .arg("leaderboard")
            .arg(0)
            .arg(-1)
            .query(&mut conn)
            .unwrap();
        assert_eq!(top, vec!["bob", "charlie", "alice"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_type_command() {
    let port = 16390;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("str", "value").unwrap();
        let _: () = conn.lpush("lst", "item").unwrap();
        let _: () = conn.sadd("st", "member").unwrap();

        let t: String = redis::cmd("TYPE").arg("str").query(&mut conn).unwrap();
        assert_eq!(t, "string");

        let t: String = redis::cmd("TYPE").arg("lst").query(&mut conn).unwrap();
        assert_eq!(t, "list");

        let t: String = redis::cmd("TYPE").arg("st").query(&mut conn).unwrap();
        assert_eq!(t, "set");

        let t: String = redis::cmd("TYPE").arg("nonexistent").query(&mut conn).unwrap();
        assert_eq!(t, "none");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_wrongtype_error() {
    let port = 16391;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mystr", "value").unwrap();
        let result: redis::RedisResult<i64> = conn.lpush("mystr", "item");
        assert!(result.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_keys_pattern() {
    let port = 16392;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("user:1:name", "Alice").unwrap();
        let _: () = conn.set("user:2:name", "Bob").unwrap();
        let _: () = conn.set("user:1:age", "30").unwrap();
        let _: () = conn.set("other", "value").unwrap();

        let mut keys: Vec<String> = redis::cmd("KEYS")
            .arg("user:*:name")
            .query(&mut conn)
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["user:1:name", "user:2:name"]);

        let mut keys: Vec<String> = redis::cmd("KEYS")
            .arg("*")
            .query(&mut conn)
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["other", "user:1:age", "user:1:name", "user:2:name"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_dbsize_flush() {
    let port = 16393;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("k1", "v1").unwrap();
        let _: () = conn.set("k2", "v2").unwrap();

        let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
        assert_eq!(size, 2);

        let _: () = redis::cmd("FLUSHDB").query(&mut conn).unwrap();

        let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
        assert_eq!(size, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_multi_exec() {
    let port = 16394;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Use pipeline for MULTI/EXEC
        let results: Vec<String> = redis::pipe()
            .atomic()
            .set("tx_key1", "val1").ignore()
            .set("tx_key2", "val2").ignore()
            .get("tx_key1")
            .get("tx_key2")
            .query(&mut conn)
            .unwrap();
        assert_eq!(results, vec!["val1", "val2"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_append_strlen() {
    let port = 16395;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let len: i64 = conn.append("mykey", "Hello").unwrap();
        assert_eq!(len, 5);

        let len: i64 = conn.append("mykey", " World").unwrap();
        assert_eq!(len, 11);

        let val: String = conn.get("mykey").unwrap();
        assert_eq!(val, "Hello World");

        let len: i64 = conn.strlen("mykey").unwrap();
        assert_eq!(len, 11);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_rename() {
    let port = 16396;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("oldkey", "myvalue").unwrap();
        let _: () = conn.rename("oldkey", "newkey").unwrap();

        let exists: bool = conn.exists("oldkey").unwrap();
        assert!(!exists);

        let val: String = conn.get("newkey").unwrap();
        assert_eq!(val, "myvalue");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_set_nx_xx() {
    let port = 16397;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // SETNX: set only if not exists
        let result: bool = conn.set_nx("nxkey", "value1").unwrap();
        assert!(result);

        let result: bool = conn.set_nx("nxkey", "value2").unwrap();
        assert!(!result);

        let val: String = conn.get("nxkey").unwrap();
        assert_eq!(val, "value1");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_info_command() {
    let port = 16398;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let info: String = redis::cmd("INFO").query(&mut conn).unwrap();
        assert!(info.contains("# Server"));
        assert!(info.contains("cedis_version"));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_config_get_set() {
    let port = 16399;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // CONFIG GET
        let result: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("timeout")
            .query(&mut conn)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "timeout");

        // CONFIG SET
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("hz")
            .arg("20")
            .query(&mut conn)
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sort_alpha() {
    let port = 16400;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("mylist", "banana").unwrap();
        let _: () = conn.rpush("mylist", "apple").unwrap();
        let _: () = conn.rpush("mylist", "cherry").unwrap();

        let sorted: Vec<String> = redis::cmd("SORT")
            .arg("mylist")
            .arg("ALPHA")
            .query(&mut conn)
            .unwrap();
        assert_eq!(sorted, vec!["apple", "banana", "cherry"]);

        let sorted_desc: Vec<String> = redis::cmd("SORT")
            .arg("mylist")
            .arg("ALPHA")
            .arg("DESC")
            .query(&mut conn)
            .unwrap();
        assert_eq!(sorted_desc, vec!["cherry", "banana", "apple"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sort_numeric() {
    let port = 16401;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("nums", "3").unwrap();
        let _: () = conn.rpush("nums", "1").unwrap();
        let _: () = conn.rpush("nums", "2").unwrap();
        let _: () = conn.rpush("nums", "10").unwrap();

        let sorted: Vec<String> = redis::cmd("SORT")
            .arg("nums")
            .query(&mut conn)
            .unwrap();
        assert_eq!(sorted, vec!["1", "2", "3", "10"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sort_limit() {
    let port = 16402;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        for i in 1..=5 {
            let _: () = conn.rpush("nums", i.to_string().as_str()).unwrap();
        }

        let sorted: Vec<String> = redis::cmd("SORT")
            .arg("nums")
            .arg("LIMIT")
            .arg("1")
            .arg("2")
            .query(&mut conn)
            .unwrap();
        assert_eq!(sorted, vec!["2", "3"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_blpop_with_data() {
    let port = 16403;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("list1", "a").unwrap();
        let _: () = conn.rpush("list1", "b").unwrap();

        let result: (String, String) = redis::cmd("BLPOP")
            .arg("list1")
            .arg("0")
            .query(&mut conn)
            .unwrap();
        assert_eq!(result, ("list1".to_string(), "a".to_string()));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_brpop_with_data() {
    let port = 16404;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("list1", "a").unwrap();
        let _: () = conn.rpush("list1", "b").unwrap();

        let result: (String, String) = redis::cmd("BRPOP")
            .arg("list1")
            .arg("0")
            .query(&mut conn)
            .unwrap();
        assert_eq!(result, ("list1".to_string(), "b".to_string()));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pubsub_publish() {
    let port = 16405;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // PUBLISH when no subscribers returns 0
        let count: i64 = redis::cmd("PUBLISH")
            .arg("mychannel")
            .arg("hello")
            .query(&mut conn)
            .unwrap();
        assert_eq!(count, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_save_command() {
    let port = 16406;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Set some data first
        let _: () = conn.set("key1", "val1").unwrap();

        // SAVE should return OK
        let result: String = redis::cmd("SAVE").query(&mut conn).unwrap();
        assert_eq!(result, "OK");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_bgsave_command() {
    let port = 16407;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let result: String = redis::cmd("BGSAVE").query(&mut conn).unwrap();
        assert_eq!(result, "Background saving started");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_lastsave_command() {
    let port = 16408;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let result: i64 = redis::cmd("LASTSAVE").query(&mut conn).unwrap();
        assert!(result > 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_select_database() {
    let port = 16409;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Set in db 0
        let _: () = conn.set("key", "db0val").unwrap();

        // Switch to db 1
        let _: () = redis::cmd("SELECT").arg("1").query(&mut conn).unwrap();
        let result: Option<String> = conn.get("key").unwrap();
        assert_eq!(result, None);

        // Set in db 1
        let _: () = conn.set("key", "db1val").unwrap();

        // Switch back to db 0
        let _: () = redis::cmd("SELECT").arg("0").query(&mut conn).unwrap();
        let val: String = conn.get("key").unwrap();
        assert_eq!(val, "db0val");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_getset() {
    let port = 16410;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // GETSET on non-existent key
        let old: Option<String> = redis::cmd("GETSET")
            .arg("mykey")
            .arg("newval")
            .query(&mut conn)
            .unwrap();
        assert_eq!(old, None);

        // GETSET replaces and returns old
        let old: String = redis::cmd("GETSET")
            .arg("mykey")
            .arg("newerval")
            .query(&mut conn)
            .unwrap();
        assert_eq!(old, "newval");

        let current: String = conn.get("mykey").unwrap();
        assert_eq!(current, "newerval");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_setex_psetex() {
    let port = 16411;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // SETEX
        let _: () = redis::cmd("SETEX")
            .arg("mykey")
            .arg("100")
            .arg("myval")
            .query(&mut conn)
            .unwrap();
        let val: String = conn.get("mykey").unwrap();
        assert_eq!(val, "myval");
        let ttl: i64 = conn.ttl("mykey").unwrap();
        assert!(ttl > 0 && ttl <= 100);

        // PSETEX
        let _: () = redis::cmd("PSETEX")
            .arg("mskey")
            .arg("100000")
            .arg("msval")
            .query(&mut conn)
            .unwrap();
        let val: String = conn.get("mskey").unwrap();
        assert_eq!(val, "msval");
        let pttl: i64 = conn.pttl("mskey").unwrap();
        assert!(pttl > 0 && pttl <= 100000);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_getrange_setrange() {
    let port = 16412;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "Hello, World!").unwrap();

        let substr: String = redis::cmd("GETRANGE")
            .arg("mykey")
            .arg("0")
            .arg("4")
            .query(&mut conn)
            .unwrap();
        assert_eq!(substr, "Hello");

        let _: i64 = redis::cmd("SETRANGE")
            .arg("mykey")
            .arg("7")
            .arg("Redis!")
            .query(&mut conn)
            .unwrap();
        let val: String = conn.get("mykey").unwrap();
        assert_eq!(val, "Hello, Redis!");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_linsert() {
    let port = 16413;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("mylist", "a").unwrap();
        let _: () = conn.rpush("mylist", "c").unwrap();

        let len: i64 = redis::cmd("LINSERT")
            .arg("mylist")
            .arg("BEFORE")
            .arg("c")
            .arg("b")
            .query(&mut conn)
            .unwrap();
        assert_eq!(len, 3);

        let items: Vec<String> = conn.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec!["a", "b", "c"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_rpoplpush() {
    let port = 16414;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.rpush("src", "a").unwrap();
        let _: () = conn.rpush("src", "b").unwrap();
        let _: () = conn.rpush("src", "c").unwrap();

        let val: String = redis::cmd("RPOPLPUSH")
            .arg("src")
            .arg("dst")
            .query(&mut conn)
            .unwrap();
        assert_eq!(val, "c");

        let dst: Vec<String> = conn.lrange("dst", 0, -1).unwrap();
        assert_eq!(dst, vec!["c"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_smembers() {
    let port = 16415;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.sadd("myset", "a").unwrap();
        let _: () = conn.sadd("myset", "b").unwrap();
        let _: () = conn.sadd("myset", "c").unwrap();

        let mut members: Vec<String> = conn.smembers("myset").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sunion_sinter_sdiff() {
    let port = 16416;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.sadd("s1", "a").unwrap();
        let _: () = conn.sadd("s1", "b").unwrap();
        let _: () = conn.sadd("s1", "c").unwrap();
        let _: () = conn.sadd("s2", "b").unwrap();
        let _: () = conn.sadd("s2", "c").unwrap();
        let _: () = conn.sadd("s2", "d").unwrap();

        let mut union: Vec<String> = redis::cmd("SUNION")
            .arg("s1")
            .arg("s2")
            .query(&mut conn)
            .unwrap();
        union.sort();
        assert_eq!(union, vec!["a", "b", "c", "d"]);

        let mut inter: Vec<String> = redis::cmd("SINTER")
            .arg("s1")
            .arg("s2")
            .query(&mut conn)
            .unwrap();
        inter.sort();
        assert_eq!(inter, vec!["b", "c"]);

        let mut diff: Vec<String> = redis::cmd("SDIFF")
            .arg("s1")
            .arg("s2")
            .query(&mut conn)
            .unwrap();
        diff.sort();
        assert_eq!(diff, vec!["a"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_zadd_zrange_withscores() {
    let port = 16417;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.zadd("zs", "alice", 1).unwrap();
        let _: () = conn.zadd("zs", "bob", 2).unwrap();
        let _: () = conn.zadd("zs", "charlie", 3).unwrap();

        let range: Vec<String> = redis::cmd("ZRANGE")
            .arg("zs")
            .arg("0")
            .arg("-1")
            .query(&mut conn)
            .unwrap();
        assert_eq!(range, vec!["alice", "bob", "charlie"]);

        let range_ws: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg("zs")
            .arg("0")
            .arg("-1")
            .arg("WITHSCORES")
            .query(&mut conn)
            .unwrap();
        assert_eq!(
            range_ws,
            vec![
                ("alice".to_string(), 1.0),
                ("bob".to_string(), 2.0),
                ("charlie".to_string(), 3.0)
            ]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_zincrby() {
    let port = 16418;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.zadd("zs", "member", 10).unwrap();

        let new_score: f64 = redis::cmd("ZINCRBY")
            .arg("zs")
            .arg("5")
            .arg("member")
            .query(&mut conn)
            .unwrap();
        assert_eq!(new_score, 15.0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_hgetall_hmget() {
    let port = 16419;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.hset("h", "f1", "v1").unwrap();
        let _: () = conn.hset("h", "f2", "v2").unwrap();
        let _: () = conn.hset("h", "f3", "v3").unwrap();

        let vals: Vec<String> = redis::cmd("HMGET")
            .arg("h")
            .arg("f1")
            .arg("f3")
            .query(&mut conn)
            .unwrap();
        assert_eq!(vals, vec!["v1", "v3"]);

        let mut all: Vec<(String, String)> = conn.hgetall("h").unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![
                ("f1".to_string(), "v1".to_string()),
                ("f2".to_string(), "v2".to_string()),
                ("f3".to_string(), "v3".to_string()),
            ]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_incrbyfloat() {
    let port = 16420;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "10.5").unwrap();
        let result: f64 = redis::cmd("INCRBYFLOAT")
            .arg("mykey")
            .arg("0.1")
            .query(&mut conn)
            .unwrap();
        assert!((result - 10.6).abs() < 0.001);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_persist() {
    let port = 16421;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "myval").unwrap();
        let _: () = conn.expire("mykey", 100).unwrap();
        assert!(conn.ttl::<_, i64>("mykey").unwrap() > 0);

        let removed: bool = conn.persist("mykey").unwrap();
        assert!(removed);

        let ttl: i64 = conn.ttl("mykey").unwrap();
        assert_eq!(ttl, -1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_multi_discard() {
    let port = 16422;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // MULTI then DISCARD
        let _: String = redis::cmd("MULTI").query(&mut conn).unwrap();
        let queued: String = redis::cmd("SET")
            .arg("key")
            .arg("val")
            .query(&mut conn)
            .unwrap();
        assert_eq!(queued, "QUEUED");

        let _: String = redis::cmd("DISCARD").query(&mut conn).unwrap();

        // Key should not exist
        let exists: bool = conn.exists("key").unwrap();
        assert!(!exists);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_time_command() {
    let port = 16423;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let result: Vec<String> = redis::cmd("TIME").query(&mut conn).unwrap();
        assert_eq!(result.len(), 2);
        let secs: u64 = result[0].parse().unwrap();
        assert!(secs > 1_000_000_000); // After year 2001
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_unknown_command() {
    let port = 16424;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let result: redis::RedisResult<String> = redis::cmd("FOOBAR").query(&mut conn);
        assert!(result.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_concurrent_clients() {
    let port = 16425;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut handles = vec![];
    for i in 0..5 {
        let handle = tokio::task::spawn_blocking(move || {
            let mut conn = get_client(port);
            let key = format!("concurrent_key_{i}");
            let val = format!("value_{i}");
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&val)
                .query(&mut conn)
                .unwrap();
            let result: String = redis::cmd("GET").arg(&key).query(&mut conn).unwrap();
            assert_eq!(result, val);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_scan() {
    let port = 16426;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        for i in 0..10 {
            let _: () = conn.set(format!("scankey:{i}"), format!("val{i}")).unwrap();
        }

        // SCAN with MATCH
        let (cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
            .arg("0")
            .arg("MATCH")
            .arg("scankey:*")
            .arg("COUNT")
            .arg("100")
            .query(&mut conn)
            .unwrap();

        // cursor should be 0 (we scanned everything)
        // OR we got partial results; either way keys should not be empty
        if cursor == "0" {
            assert_eq!(keys.len(), 10);
        } else {
            assert!(!keys.is_empty());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_msetnx() {
    let port = 16427;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // MSETNX succeeds when no keys exist
        let result: bool = redis::cmd("MSETNX")
            .arg("k1")
            .arg("v1")
            .arg("k2")
            .arg("v2")
            .query(&mut conn)
            .unwrap();
        assert!(result);

        // MSETNX fails when any key exists
        let result: bool = redis::cmd("MSETNX")
            .arg("k2")
            .arg("v2new")
            .arg("k3")
            .arg("v3")
            .query(&mut conn)
            .unwrap();
        assert!(!result);

        // k3 should not exist (since MSETNX was atomic and failed)
        let exists: bool = conn.exists("k3").unwrap();
        assert!(!exists);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_hincrby() {
    let port = 16428;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.hset("myhash", "counter", "10").unwrap();
        let new_val: i64 = conn.hincr("myhash", "counter", 5).unwrap();
        assert_eq!(new_val, 15);

        let new_val: i64 = conn.hincr("myhash", "counter", -3).unwrap();
        assert_eq!(new_val, 12);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_getdel() {
    let port = 16429;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = conn.set("mykey", "myval").unwrap();
        let val: String = redis::cmd("GETDEL").arg("mykey").query(&mut conn).unwrap();
        assert_eq!(val, "myval");

        let exists: bool = conn.exists("mykey").unwrap();
        assert!(!exists);
    })
    .await
    .unwrap();
}

// =========== Bitmap tests ===========

#[tokio::test]
async fn test_setbit_getbit() {
    let port = 16430;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // SETBIT returns old value (0)
        let old: i64 = redis::cmd("SETBIT").arg("bm").arg(7).arg(1).query(&mut conn).unwrap();
        assert_eq!(old, 0);

        // GETBIT should return 1
        let bit: i64 = redis::cmd("GETBIT").arg("bm").arg(7).query(&mut conn).unwrap();
        assert_eq!(bit, 1);

        // GETBIT on unset bit returns 0
        let bit: i64 = redis::cmd("GETBIT").arg("bm").arg(0).query(&mut conn).unwrap();
        assert_eq!(bit, 0);

        // SETBIT again returns old value (1)
        let old: i64 = redis::cmd("SETBIT").arg("bm").arg(7).arg(0).query(&mut conn).unwrap();
        assert_eq!(old, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_bitcount() {
    let port = 16431;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: () = redis::cmd("SET").arg("mykey").arg("foobar").query(&mut conn).unwrap();
        let count: i64 = redis::cmd("BITCOUNT").arg("mykey").query(&mut conn).unwrap();
        assert!(count > 0);

        // BITCOUNT with range
        let count: i64 = redis::cmd("BITCOUNT")
            .arg("mykey")
            .arg(0)
            .arg(0)
            .query(&mut conn)
            .unwrap();
        assert!(count > 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_bitop() {
    let port = 16432;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: i64 = redis::cmd("SETBIT").arg("a").arg(0).arg(1).query(&mut conn).unwrap();
        let _: i64 = redis::cmd("SETBIT").arg("a").arg(1).arg(1).query(&mut conn).unwrap();
        let _: i64 = redis::cmd("SETBIT").arg("b").arg(0).arg(1).query(&mut conn).unwrap();
        let _: i64 = redis::cmd("SETBIT").arg("b").arg(2).arg(1).query(&mut conn).unwrap();

        // AND
        let len: i64 = redis::cmd("BITOP")
            .arg("AND")
            .arg("dest")
            .arg("a")
            .arg("b")
            .query(&mut conn)
            .unwrap();
        assert!(len > 0);
        let bit: i64 = redis::cmd("GETBIT").arg("dest").arg(0).query(&mut conn).unwrap();
        assert_eq!(bit, 1); // Both have bit 0 set
        let bit: i64 = redis::cmd("GETBIT").arg("dest").arg(1).query(&mut conn).unwrap();
        assert_eq!(bit, 0); // Only a has bit 1

        // OR
        let _: i64 = redis::cmd("BITOP")
            .arg("OR")
            .arg("dest2")
            .arg("a")
            .arg("b")
            .query(&mut conn)
            .unwrap();
        let bit: i64 = redis::cmd("GETBIT").arg("dest2").arg(1).query(&mut conn).unwrap();
        assert_eq!(bit, 1);
        let bit: i64 = redis::cmd("GETBIT").arg("dest2").arg(2).query(&mut conn).unwrap();
        assert_eq!(bit, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_bitpos() {
    let port = 16433;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: i64 = redis::cmd("SETBIT").arg("bm").arg(10).arg(1).query(&mut conn).unwrap();

        let pos: i64 = redis::cmd("BITPOS").arg("bm").arg(1).query(&mut conn).unwrap();
        assert_eq!(pos, 10);

        let pos: i64 = redis::cmd("BITPOS").arg("bm").arg(0).query(&mut conn).unwrap();
        assert_eq!(pos, 0);
    })
    .await
    .unwrap();
}

// =========== HyperLogLog tests ===========

#[tokio::test]
async fn test_pfadd_pfcount() {
    let port = 16434;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let changed: i64 = redis::cmd("PFADD")
            .arg("hll")
            .arg("a")
            .arg("b")
            .arg("c")
            .query(&mut conn)
            .unwrap();
        assert_eq!(changed, 1);

        let count: i64 = redis::cmd("PFCOUNT").arg("hll").query(&mut conn).unwrap();
        assert!(count >= 2 && count <= 4); // HLL is approximate

        // Adding duplicate should return 0
        let changed: i64 = redis::cmd("PFADD")
            .arg("hll")
            .arg("a")
            .query(&mut conn)
            .unwrap();
        assert_eq!(changed, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pfmerge() {
    let port = 16435;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: i64 = redis::cmd("PFADD")
            .arg("hll1")
            .arg("a")
            .arg("b")
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("PFADD")
            .arg("hll2")
            .arg("c")
            .arg("d")
            .query(&mut conn)
            .unwrap();

        let _: String = redis::cmd("PFMERGE")
            .arg("merged")
            .arg("hll1")
            .arg("hll2")
            .query(&mut conn)
            .unwrap();

        let count: i64 = redis::cmd("PFCOUNT").arg("merged").query(&mut conn).unwrap();
        assert!(count >= 3 && count <= 5); // Should be ~4
    })
    .await
    .unwrap();
}

// =========== Stream tests ===========

#[tokio::test]
async fn test_xadd_xlen() {
    let port = 16436;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // XADD with auto-generated ID
        let id: String = redis::cmd("XADD")
            .arg("stream")
            .arg("*")
            .arg("name")
            .arg("Alice")
            .arg("age")
            .arg("30")
            .query(&mut conn)
            .unwrap();
        assert!(id.contains('-')); // ID format is ms-seq

        let _: String = redis::cmd("XADD")
            .arg("stream")
            .arg("*")
            .arg("name")
            .arg("Bob")
            .query(&mut conn)
            .unwrap();

        let len: i64 = redis::cmd("XLEN").arg("stream").query(&mut conn).unwrap();
        assert_eq!(len, 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_xrange() {
    let port = 16437;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        let _: String = redis::cmd("XADD")
            .arg("mystream")
            .arg("*")
            .arg("field1")
            .arg("value1")
            .query(&mut conn)
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg("mystream")
            .arg("*")
            .arg("field2")
            .arg("value2")
            .query(&mut conn)
            .unwrap();

        // XRANGE - get all entries
        let entries: Vec<redis::Value> = redis::cmd("XRANGE")
            .arg("mystream")
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .unwrap();
        assert_eq!(entries.len(), 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_xtrim() {
    let port = 16438;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Add 5 entries
        for i in 0..5 {
            let _: String = redis::cmd("XADD")
                .arg("mystream")
                .arg("*")
                .arg("i")
                .arg(i.to_string())
                .query(&mut conn)
                .unwrap();
        }

        let len: i64 = redis::cmd("XLEN").arg("mystream").query(&mut conn).unwrap();
        assert_eq!(len, 5);

        // XTRIM to 2
        let trimmed: i64 = redis::cmd("XTRIM")
            .arg("mystream")
            .arg("MAXLEN")
            .arg(2)
            .query(&mut conn)
            .unwrap();
        assert_eq!(trimmed, 3);

        let len: i64 = redis::cmd("XLEN").arg("mystream").query(&mut conn).unwrap();
        assert_eq!(len, 2);
    })
    .await
    .unwrap();
}

// =========== True blocking BLPOP test ===========

#[tokio::test]
async fn test_blpop_blocking() {
    let port = 16439;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Spawn a task that will BLPOP on an empty key, then another that pushes to it
    let blpop_handle = tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let start = Instant::now();

        // BLPOP with 5 second timeout on an empty key
        let result: Vec<String> = redis::cmd("BLPOP")
            .arg("blockkey")
            .arg(5)
            .query(&mut conn)
            .unwrap();

        let elapsed = start.elapsed();
        // Should have blocked for some time (at least 100ms) before getting data
        assert!(elapsed.as_millis() >= 50, "BLPOP returned too quickly: {elapsed:?}");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "blockkey");
        assert_eq!(result[1], "hello");
    });

    // Wait a bit, then push data from another connection
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let _: () = conn.lpush("blockkey", "hello").unwrap();
    })
    .await
    .unwrap();

    blpop_handle.await.unwrap();
}

#[tokio::test]
async fn test_blpop_timeout() {
    let port = 16440;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let start = Instant::now();

        // BLPOP with 1 second timeout on nonexistent key
        let result: redis::Value = redis::cmd("BLPOP")
            .arg("nokey")
            .arg(1)
            .query(&mut conn)
            .unwrap();

        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 900, "BLPOP timed out too quickly: {elapsed:?}");
        assert_eq!(result, redis::Value::Nil);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_brpop_blocking() {
    let port = 16441;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let brpop_handle = tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let start = Instant::now();

        let result: Vec<String> = redis::cmd("BRPOP")
            .arg("brkey")
            .arg(5)
            .query(&mut conn)
            .unwrap();

        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 50, "BRPOP returned too quickly");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "brkey");
        assert_eq!(result[1], "world");
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let _: () = conn.rpush("brkey", "world").unwrap();
    })
    .await
    .unwrap();

    brpop_handle.await.unwrap();
}

// =========== Config / eviction test ===========

#[tokio::test]
async fn test_config_maxmemory() {
    let port = 16442;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Set maxmemory via CONFIG SET
        let _: String = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg("1000000")
            .query(&mut conn)
            .unwrap();

        // Verify via CONFIG GET
        let result: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory")
            .query(&mut conn)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "maxmemory");
        assert_eq!(result[1], "1000000");

        // Set maxmemory-policy
        let _: String = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory-policy")
            .arg("allkeys-random")
            .query(&mut conn)
            .unwrap();

        let result: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory-policy")
            .query(&mut conn)
            .unwrap();
        assert_eq!(result[1], "allkeys-random");
    })
    .await
    .unwrap();
}

// =========== XREAD test ===========

#[tokio::test]
async fn test_xread() {
    let port = 16443;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);

        // Add entries
        let id1: String = redis::cmd("XADD")
            .arg("s1")
            .arg("*")
            .arg("k")
            .arg("v1")
            .query(&mut conn)
            .unwrap();

        let _: String = redis::cmd("XADD")
            .arg("s1")
            .arg("*")
            .arg("k")
            .arg("v2")
            .query(&mut conn)
            .unwrap();

        // XREAD: read entries after the first one
        let result: redis::Value = redis::cmd("XREAD")
            .arg("STREAMS")
            .arg("s1")
            .arg(&id1)
            .query(&mut conn)
            .unwrap();

        // Should return a non-nil result with entries after id1
        assert_ne!(result, redis::Value::Nil);
    })
    .await
    .unwrap();
}
