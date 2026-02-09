use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::resp::RespValue;
use crate::store::SharedStore;

pub fn cmd_multi(client: &mut ClientState) -> RespValue {
    if client.in_multi {
        return RespValue::error("ERR MULTI calls can not be nested");
    }
    client.in_multi = true;
    client.multi_queue.clear();
    RespValue::ok()
}

pub fn cmd_exec<'a>(
    store: &'a SharedStore,
    config: &'a SharedConfig,
    client: &'a mut ClientState,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = RespValue> + Send + 'a>> {
    Box::pin(async move {
        if !client.in_multi {
            return RespValue::error("ERR EXEC without MULTI");
        }

        client.in_multi = false;

        // Check WATCH: if any watched key was modified, abort
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
            let result =
                crate::command::dispatch(&cmd_name, &args, store, config, client).await;
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
    RespValue::ok()
}

pub async fn cmd_watch(
    args: &[RespValue],
    _store: &SharedStore,
    client: &mut ClientState,
) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR wrong number of arguments for 'watch' command");
    }
    if client.in_multi {
        return RespValue::error("ERR WATCH inside MULTI is not allowed");
    }

    for arg in args {
        if let Some(key) = crate::command::arg_to_string(arg) {
            client.watched_keys.push(key);
        }
    }

    RespValue::ok()
}

pub fn cmd_unwatch(client: &mut ClientState) -> RespValue {
    client.watched_keys.clear();
    client.watch_dirty = false;
    RespValue::ok()
}
