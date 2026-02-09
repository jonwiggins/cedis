use cedis::config::Config;
use cedis::server;
use cedis::store::DataStore;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    let config = Arc::new(RwLock::new(config));
    let store = Arc::new(RwLock::new(DataStore::new(num_dbs)));

    server::run_server(store, config).await
}
