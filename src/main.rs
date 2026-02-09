use cedis::config::Config;
use cedis::persistence::aof::{AofWriter, FsyncPolicy};
use cedis::persistence::rdb;
use cedis::pubsub::PubSubRegistry;
use cedis::server;
use cedis::store::DataStore;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::info;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    // Parse command line args
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = Config::from_args(&args);

    let num_dbs = config.databases;
    let rdb_path = format!("{}/{}", config.dir, config.dbfilename);
    let aof_enabled = config.appendonly;
    let aof_path = format!("{}/appendonly.aof", config.dir);
    let aof_policy = FsyncPolicy::from_str(&config.appendfsync);

    // Try to load RDB on startup
    let store = if std::path::Path::new(&rdb_path).exists() {
        info!("Loading RDB from {rdb_path}...");
        match rdb::load(&rdb_path, num_dbs) {
            Ok(store) => {
                info!("RDB loaded successfully");
                store
            }
            Err(e) => {
                tracing::warn!("Failed to load RDB: {e}, starting with empty store");
                DataStore::new(num_dbs)
            }
        }
    } else {
        DataStore::new(num_dbs)
    };

    let mut store = store;

    // Try to replay AOF if it exists (AOF takes precedence over RDB)
    if aof_enabled && std::path::Path::new(&aof_path).exists() {
        info!("Replaying AOF from {aof_path}...");
        match cedis::persistence::aof::replay(&aof_path, &mut store, num_dbs) {
            Ok(count) => info!("AOF replayed {count} commands"),
            Err(e) => tracing::warn!("Failed to replay AOF: {e}"),
        }
    }

    let config = Arc::new(RwLock::new(config));
    let store = Arc::new(RwLock::new(store));
    let pubsub = Arc::new(RwLock::new(PubSubRegistry::new()));

    // Set up AOF writer
    let mut aof_writer = AofWriter::new();
    if aof_enabled {
        if let Err(e) = aof_writer.open(&aof_path, aof_policy) {
            tracing::warn!("Failed to open AOF: {e}");
        } else {
            info!("AOF enabled: {aof_path}");
        }
    }
    let aof = Arc::new(Mutex::new(aof_writer));

    server::run_server(store, config, pubsub, aof).await
}
