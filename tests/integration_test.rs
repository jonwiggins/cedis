use redis::Commands;
use std::sync::Arc;
use tokio::sync::RwLock;

fn start_server(port: u16) -> tokio::task::JoinHandle<()> {
    let config = cedis::config::Config {
        port,
        ..Default::default()
    };
    let num_dbs = config.databases;
    let config = Arc::new(RwLock::new(config));
    let store = Arc::new(RwLock::new(cedis::store::DataStore::new(num_dbs)));

    tokio::spawn(async move {
        let _ = cedis::server::run_server(store, config).await;
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
