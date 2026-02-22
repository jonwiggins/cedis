use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub requirepass: Option<String>,
    pub timeout: u64,
    pub tcp_keepalive: u64,
    pub hz: u64,
    pub loglevel: String,
    // Persistence
    pub dbfilename: String,
    pub dir: String,
    pub appendonly: bool,
    pub appendfsync: String,
    pub save_rules: Vec<(u64, u64)>,
    // Memory
    pub maxmemory: u64,
    pub maxmemory_policy: String,
    // Encoding thresholds
    pub list_max_listpack_size: i64,
    pub hash_max_listpack_entries: u64,
    pub hash_max_listpack_value: u64,
    pub set_max_intset_entries: u64,
    pub set_max_listpack_entries: u64,
    pub set_max_listpack_value: u64,
    pub list_compress_depth: i64,
    pub zset_max_listpack_entries: u64,
    pub zset_max_listpack_value: u64,
    // Debug flags
    pub active_expire_enabled: bool,
    // Replication
    pub replicaof: Option<(String, u16)>,
    pub replica_read_only: bool,
    pub repl_backlog_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            databases: 16,
            requirepass: None,
            timeout: 0,
            tcp_keepalive: 300,
            hz: 10,
            loglevel: "notice".to_string(),
            dbfilename: "dump.rdb".to_string(),
            dir: ".".to_string(),
            appendonly: false,
            appendfsync: "everysec".to_string(),
            save_rules: vec![(900, 1), (300, 10), (60, 10000)],
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_string(),
            list_max_listpack_size: -2,
            hash_max_listpack_entries: 128,
            hash_max_listpack_value: 64,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            list_compress_depth: 0,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            active_expire_enabled: true,
            replicaof: None,
            replica_read_only: true,
            repl_backlog_size: 1_048_576, // 1MB
        }
    }
}

impl Config {
    pub fn from_args(args: &[String]) -> Self {
        let mut config = Config::default();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    if i + 1 < args.len() {
                        if let Ok(p) = args[i + 1].parse() {
                            config.port = p;
                        }
                        i += 1;
                    }
                }
                "--bind" => {
                    if i + 1 < args.len() {
                        config.bind = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--requirepass" => {
                    if i + 1 < args.len() {
                        config.requirepass = Some(args[i + 1].clone());
                        i += 1;
                    }
                }
                "--dbfilename" => {
                    if i + 1 < args.len() {
                        config.dbfilename = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--dir" => {
                    if i + 1 < args.len() {
                        config.dir = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--appendonly" => {
                    if i + 1 < args.len() {
                        config.appendonly = args[i + 1] == "yes";
                        i += 1;
                    }
                }
                "--databases" => {
                    if i + 1 < args.len() {
                        if let Ok(d) = args[i + 1].parse() {
                            config.databases = d;
                        }
                        i += 1;
                    }
                }
                "--timeout" => {
                    if i + 1 < args.len() {
                        if let Ok(t) = args[i + 1].parse() {
                            config.timeout = t;
                        }
                        i += 1;
                    }
                }
                "--loglevel" => {
                    if i + 1 < args.len() {
                        config.loglevel = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--hz" => {
                    if i + 1 < args.len() {
                        if let Ok(h) = args[i + 1].parse() {
                            config.hz = h;
                        }
                        i += 1;
                    }
                }
                "--replicaof" | "--slaveof" => {
                    if i + 2 < args.len() {
                        let host = args[i + 1].clone();
                        if let Ok(port) = args[i + 2].parse::<u16>() {
                            if host.eq_ignore_ascii_case("no")
                                && args[i + 2].eq_ignore_ascii_case("one")
                            {
                                config.replicaof = None;
                            } else {
                                config.replicaof = Some((host, port));
                            }
                        }
                        i += 2;
                    }
                }
                "--repl-backlog-size" => {
                    if i + 1 < args.len() {
                        if let Ok(s) = args[i + 1].parse() {
                            config.repl_backlog_size = s;
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        config
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match key.to_lowercase().as_str() {
            "bind" => Some(self.bind.clone()),
            "port" => Some(self.port.to_string()),
            "databases" => Some(self.databases.to_string()),
            "requirepass" => self.requirepass.clone().or(Some(String::new())),
            "timeout" => Some(self.timeout.to_string()),
            "tcp-keepalive" => Some(self.tcp_keepalive.to_string()),
            "hz" => Some(self.hz.to_string()),
            "loglevel" => Some(self.loglevel.clone()),
            "dbfilename" => Some(self.dbfilename.clone()),
            "dir" => Some(self.dir.clone()),
            "appendonly" => Some(if self.appendonly { "yes" } else { "no" }.to_string()),
            "appendfsync" => Some(self.appendfsync.clone()),
            "maxmemory" => Some(self.maxmemory.to_string()),
            "maxmemory-policy" => Some(self.maxmemory_policy.clone()),
            "list-max-ziplist-size" | "list-max-listpack-size" => {
                Some(self.list_max_listpack_size.to_string())
            }
            "hash-max-ziplist-entries" | "hash-max-listpack-entries" => {
                Some(self.hash_max_listpack_entries.to_string())
            }
            "hash-max-ziplist-value" | "hash-max-listpack-value" => {
                Some(self.hash_max_listpack_value.to_string())
            }
            "set-max-intset-entries" => Some(self.set_max_intset_entries.to_string()),
            "set-max-listpack-entries" => Some(self.set_max_listpack_entries.to_string()),
            "set-max-listpack-value" => Some(self.set_max_listpack_value.to_string()),
            "list-compress-depth" => Some(self.list_compress_depth.to_string()),
            "zset-max-ziplist-entries" | "zset-max-listpack-entries" => {
                Some(self.zset_max_listpack_entries.to_string())
            }
            "zset-max-ziplist-value" | "zset-max-listpack-value" => {
                Some(self.zset_max_listpack_value.to_string())
            }
            "save" => {
                let s: Vec<String> = self
                    .save_rules
                    .iter()
                    .map(|(secs, changes)| format!("{secs} {changes}"))
                    .collect();
                Some(s.join(" "))
            }
            "repl-backlog-size" => Some(self.repl_backlog_size.to_string()),
            "replica-read-only" | "slave-read-only" => {
                Some(if self.replica_read_only { "yes" } else { "no" }.to_string())
            }
            _ => None,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
        match key.to_lowercase().as_str() {
            "hz" => {
                self.hz = value.parse().map_err(|_| "Invalid hz value".to_string())?;
                Ok(())
            }
            "timeout" => {
                self.timeout = value
                    .parse()
                    .map_err(|_| "Invalid timeout value".to_string())?;
                Ok(())
            }
            "loglevel" => {
                self.loglevel = value.to_string();
                Ok(())
            }
            "maxmemory" => {
                self.maxmemory = value
                    .parse()
                    .map_err(|_| "Invalid maxmemory value".to_string())?;
                Ok(())
            }
            "maxmemory-policy" => {
                self.maxmemory_policy = value.to_string();
                Ok(())
            }
            "appendonly" => {
                self.appendonly = value == "yes";
                Ok(())
            }
            "appendfsync" => {
                self.appendfsync = value.to_string();
                Ok(())
            }
            "requirepass" => {
                self.requirepass = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
                Ok(())
            }
            "list-max-ziplist-size" | "list-max-listpack-size" => {
                self.list_max_listpack_size =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "hash-max-ziplist-entries" | "hash-max-listpack-entries" => {
                self.hash_max_listpack_entries =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "hash-max-ziplist-value" | "hash-max-listpack-value" => {
                self.hash_max_listpack_value =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "set-max-intset-entries" => {
                self.set_max_intset_entries =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "set-max-listpack-entries" => {
                self.set_max_listpack_entries =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "set-max-listpack-value" => {
                self.set_max_listpack_value =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "list-compress-depth" => {
                self.list_compress_depth =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "zset-max-ziplist-entries" | "zset-max-listpack-entries" => {
                self.zset_max_listpack_entries =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            "zset-max-ziplist-value" | "zset-max-listpack-value" => {
                self.zset_max_listpack_value =
                    value.parse().map_err(|_| "Invalid value".to_string())?;
                Ok(())
            }
            _ => {
                // Accept unknown parameters silently for compatibility
                Ok(())
            }
        }
    }
}

pub type SharedConfig = Arc<RwLock<Config>>;
