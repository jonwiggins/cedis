//! Command handlers for EVAL, EVALSHA, and SCRIPT.

use crate::command::{arg_to_i64, arg_to_string, wrong_arg_count};
use crate::config::SharedConfig;
use crate::connection::ClientState;
use crate::keywatcher::SharedKeyWatcher;
use crate::pubsub::SharedPubSub;
use crate::resp::RespValue;
use crate::scripting::{self, ScriptCache};
use crate::store::SharedStore;
use tokio::sync::mpsc;

/// EVAL script numkeys key [key ...] arg [arg ...]
#[allow(clippy::too_many_arguments)]
pub async fn cmd_eval(
    args: &[RespValue],
    store: &SharedStore,
    _config: &SharedConfig,
    client: &mut ClientState,
    _pubsub: &SharedPubSub,
    _pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    _key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("eval");
    }

    let script = match arg_to_string(&args[0]) {
        Some(s) => s,
        None => return RespValue::error("ERR invalid script"),
    };

    let numkeys = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if args.len() < 2 + numkeys {
        return RespValue::error("ERR Number of keys can't be greater than number of args");
    }

    // Cache the script
    script_cache.load(&script);

    // Extract KEYS and ARGV
    let keys: Vec<String> = args[2..2 + numkeys]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let argv: Vec<String> = args[2 + numkeys..]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    // Acquire write lock and run script synchronously
    let mut store_guard = store.write().await;
    scripting::eval_script(&script, &keys, &argv, &mut store_guard, client.db_index)
}

/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
#[allow(clippy::too_many_arguments)]
pub async fn cmd_evalsha(
    args: &[RespValue],
    store: &SharedStore,
    _config: &SharedConfig,
    client: &mut ClientState,
    _pubsub: &SharedPubSub,
    _pubsub_tx: &mpsc::UnboundedSender<RespValue>,
    _key_watcher: &SharedKeyWatcher,
    script_cache: &ScriptCache,
) -> RespValue {
    if args.len() < 2 {
        return wrong_arg_count("evalsha");
    }

    let sha = match arg_to_string(&args[0]) {
        Some(s) => s.to_lowercase(),
        None => return RespValue::error("ERR invalid SHA1"),
    };

    let numkeys = match arg_to_i64(&args[1]) {
        Some(n) if n >= 0 => n as usize,
        _ => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if args.len() < 2 + numkeys {
        return RespValue::error("ERR Number of keys can't be greater than number of args");
    }

    // Look up the cached script
    let script = match script_cache.get(&sha) {
        Some(s) => s,
        None => return RespValue::error("NOSCRIPT No matching script. Use EVAL."),
    };

    // Extract KEYS and ARGV
    let keys: Vec<String> = args[2..2 + numkeys]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    let argv: Vec<String> = args[2 + numkeys..]
        .iter()
        .filter_map(arg_to_string)
        .collect();

    // Acquire write lock and run script synchronously
    let mut store_guard = store.write().await;
    scripting::eval_script(&script, &keys, &argv, &mut store_guard, client.db_index)
}

/// SCRIPT LOAD script | SCRIPT EXISTS sha1 [sha1 ...] | SCRIPT FLUSH
pub async fn cmd_script(args: &[RespValue], script_cache: &ScriptCache) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("script");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR unknown subcommand or wrong number of arguments"),
    };

    match subcmd.as_str() {
        "LOAD" => {
            if args.len() != 2 {
                return wrong_arg_count("script|load");
            }
            let script = match arg_to_string(&args[1]) {
                Some(s) => s,
                None => return RespValue::error("ERR invalid script"),
            };
            let sha = script_cache.load(&script);
            RespValue::bulk_string(sha)
        }

        "EXISTS" => {
            if args.len() < 2 {
                return wrong_arg_count("script|exists");
            }
            let results: Vec<RespValue> = args[1..]
                .iter()
                .map(|arg| {
                    let sha = arg_to_string(arg).unwrap_or_default().to_lowercase();
                    RespValue::integer(if script_cache.exists(&sha) { 1 } else { 0 })
                })
                .collect();
            RespValue::array(results)
        }

        "FLUSH" => {
            script_cache.flush();
            RespValue::ok()
        }

        _ => RespValue::error(format!(
            "ERR unknown subcommand '{subcmd}'. Try SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH."
        )),
    }
}
