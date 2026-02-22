use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::pubsub::SharedPubSub;
use crate::resp::RespValue;
use crate::scripting::ScriptCache;
use crate::store::SharedStore;
use tokio::sync::mpsc;

pub fn cmd_multi(client: &mut ClientState) -> RespValue {
    if client.in_multi {
        return RespValue::error("ERR MULTI calls can not be nested");
    }
    client.in_multi = true;
    client.multi_queue.clear();
    client.multi_error = false;
    RespValue::ok()
}

pub fn cmd_exec<'a>(
    store: &'a SharedStore,
    config: &'a SharedConfig,
    client: &'a mut ClientState,
    pubsub: &'a SharedPubSub,
    pubsub_tx: &'a mpsc::UnboundedSender<RespValue>,
    key_watcher: &'a SharedKeyWatcher,
    script_cache: &'a ScriptCache,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = RespValue> + Send + 'a>> {
    Box::pin(async move {
        if !client.in_multi {
            return RespValue::error("ERR EXEC without MULTI");
        }

        client.in_multi = false;

        // Check EXECABORT: if any queued command had an arity error
        if client.multi_error {
            client.multi_queue.clear();
            client.watched_keys.clear();
            client.watch_dirty = false;
            client.multi_error = false;
            return RespValue::error("EXECABORT Transaction discarded because of previous errors.");
        }

        // Check WATCH: compare key state (alive/dead + version).
        // A key's "state" is whether it logically exists (not expired) and its version.
        // If the state at WATCH time matches the state at EXEC time, no conflict.
        if !client.watched_keys.is_empty() {
            let store_guard = store.read().await;
            for (db_index, key, saved_version, _saved_global, alive_at_watch) in
                &client.watched_keys
            {
                let db = &store_guard.databases[*db_index];
                let alive_now = db.key_alive(key);
                let dirty = if *alive_at_watch && alive_now {
                    // Key was alive at WATCH and is still alive: check version
                    db.key_version(key) != *saved_version
                } else if !*alive_at_watch && !alive_now {
                    // Key was dead at WATCH and is still dead: no change
                    false
                } else {
                    // Key's existence status changed (alive→dead or dead→alive)
                    true
                };
                if dirty {
                    drop(store_guard);
                    client.multi_queue.clear();
                    client.watched_keys.clear();
                    client.watch_dirty = false;
                    return RespValue::null_array();
                }
            }
            drop(store_guard);
        }

        if client.watch_dirty {
            client.multi_queue.clear();
            client.watched_keys.clear();
            client.watch_dirty = false;
            return RespValue::null_array();
        }

        let queue = std::mem::take(&mut client.multi_queue);
        client.watched_keys.clear();
        client.watch_dirty = false;

        let mut results = Vec::with_capacity(queue.len());
        for (cmd_name, args) in queue {
            let result = crate::command::dispatch(
                &cmd_name,
                &args,
                store,
                config,
                client,
                pubsub,
                pubsub_tx,
                key_watcher,
                script_cache,
            )
            .await;
            results.push(result);
        }

        RespValue::array(results)
    })
}

pub fn cmd_discard(client: &mut ClientState) -> RespValue {
    if !client.in_multi {
        return RespValue::error("ERR DISCARD without MULTI");
    }
    client.in_multi = false;
    client.multi_queue.clear();
    client.multi_error = false;
    client.watched_keys.clear();
    client.watch_dirty = false;
    RespValue::ok()
}

pub async fn cmd_watch(
    args: &[RespValue],
    store: &SharedStore,
    client: &mut ClientState,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR wrong number of arguments for 'watch' command");
    }
    if client.in_multi {
        return RespValue::error("ERR WATCH inside MULTI is not allowed");
    }

    let store_guard = store.read().await;
    let db = &store_guard.databases[client.db_index];
    let global_ver = db.global_version();

    for arg in args {
        if let Some(key) = crate::command::arg_to_string(arg) {
            let alive = db.key_alive(&key);
            let ver = db.key_version(&key);
            client
                .watched_keys
                .push((client.db_index, key, ver, global_ver, alive));
        }
    }

    RespValue::ok()
}

pub fn cmd_unwatch(client: &mut ClientState) -> RespValue {
    client.watched_keys.clear();
    client.watch_dirty = false;
    RespValue::ok()
}
