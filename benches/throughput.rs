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

fn bench_set_get(conn: &mut redis::Connection, iterations: usize) -> (f64, f64) {
    // Benchmark SET
    let start = Instant::now();
    for i in 0..iterations {
        let _: () = conn
            .set(format!("bench_key_{i}"), format!("value_{i}"))
            .unwrap();
    }
    let set_elapsed = start.elapsed();
    let set_ops = iterations as f64 / set_elapsed.as_secs_f64();

    // Benchmark GET
    let start = Instant::now();
    for i in 0..iterations {
        let _: String = conn.get(format!("bench_key_{i}")).unwrap();
    }
    let get_elapsed = start.elapsed();
    let get_ops = iterations as f64 / get_elapsed.as_secs_f64();

    (set_ops, get_ops)
}

fn bench_incr(conn: &mut redis::Connection, iterations: usize) -> f64 {
    let _: () = conn.set("bench_counter", "0").unwrap();
    let start = Instant::now();
    for _ in 0..iterations {
        let _: i64 = conn.incr("bench_counter", 1).unwrap();
    }
    let elapsed = start.elapsed();
    iterations as f64 / elapsed.as_secs_f64()
}

fn bench_lpush_lpop(conn: &mut redis::Connection, iterations: usize) -> (f64, f64) {
    let start = Instant::now();
    for i in 0..iterations {
        let _: () = conn.lpush("bench_list", format!("item_{i}")).unwrap();
    }
    let push_elapsed = start.elapsed();
    let push_ops = iterations as f64 / push_elapsed.as_secs_f64();

    let start = Instant::now();
    for _ in 0..iterations {
        let _: String = conn.lpop("bench_list", None).unwrap();
    }
    let pop_elapsed = start.elapsed();
    let pop_ops = iterations as f64 / pop_elapsed.as_secs_f64();

    (push_ops, pop_ops)
}

fn bench_hset_hget(conn: &mut redis::Connection, iterations: usize) -> (f64, f64) {
    let start = Instant::now();
    for i in 0..iterations {
        let _: () = conn
            .hset("bench_hash", format!("field_{i}"), format!("value_{i}"))
            .unwrap();
    }
    let hset_elapsed = start.elapsed();
    let hset_ops = iterations as f64 / hset_elapsed.as_secs_f64();

    let start = Instant::now();
    for i in 0..iterations {
        let _: String = conn.hget("bench_hash", format!("field_{i}")).unwrap();
    }
    let hget_elapsed = start.elapsed();
    let hget_ops = iterations as f64 / hget_elapsed.as_secs_f64();

    (hset_ops, hget_ops)
}

fn bench_sadd(conn: &mut redis::Connection, iterations: usize) -> f64 {
    let start = Instant::now();
    for i in 0..iterations {
        let _: () = conn.sadd("bench_set", format!("member_{i}")).unwrap();
    }
    let elapsed = start.elapsed();
    iterations as f64 / elapsed.as_secs_f64()
}

fn bench_pipeline(conn: &mut redis::Connection, iterations: usize) -> f64 {
    let start = Instant::now();
    let batch_size = 100;
    for batch in 0..(iterations / batch_size) {
        let mut pipe = redis::pipe();
        for i in 0..batch_size {
            let key = format!("pipe_key_{}_{}", batch, i);
            pipe.set(&key, "value").ignore();
        }
        let _: () = pipe.query(conn).unwrap();
    }
    let elapsed = start.elapsed();
    iterations as f64 / elapsed.as_secs_f64()
}

#[tokio::main]
async fn main() {
    let port = 17000;
    let _server = start_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    tokio::task::spawn_blocking(move || {
        let mut conn = get_client(port);
        let iterations = 10_000;

        println!("=== Cedis Benchmark ({iterations} operations) ===\n");

        let (set_ops, get_ops) = bench_set_get(&mut conn, iterations);
        println!("SET:    {set_ops:>10.0} ops/sec");
        println!("GET:    {get_ops:>10.0} ops/sec");

        let incr_ops = bench_incr(&mut conn, iterations);
        println!("INCR:   {incr_ops:>10.0} ops/sec");

        let (push_ops, pop_ops) = bench_lpush_lpop(&mut conn, iterations);
        println!("LPUSH:  {push_ops:>10.0} ops/sec");
        println!("LPOP:   {pop_ops:>10.0} ops/sec");

        let (hset_ops, hget_ops) = bench_hset_hget(&mut conn, iterations);
        println!("HSET:   {hset_ops:>10.0} ops/sec");
        println!("HGET:   {hget_ops:>10.0} ops/sec");

        let sadd_ops = bench_sadd(&mut conn, iterations);
        println!("SADD:   {sadd_ops:>10.0} ops/sec");

        let pipe_ops = bench_pipeline(&mut conn, iterations);
        println!("PIPE:   {pipe_ops:>10.0} ops/sec (100-cmd pipeline batches)");

        println!("\n=== Done ===");
    })
    .await
    .unwrap();
}
