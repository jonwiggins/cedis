use crate::command::{arg_to_bytes, arg_to_string, wrong_arg_count};
use crate::connection::ClientState;
use crate::pubsub::SharedPubSub;
use crate::resp::RespValue;
use tokio::sync::mpsc;

pub async fn cmd_subscribe(
    args: &[RespValue],
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("subscribe");
    }

    let mut responses = Vec::new();
    let mut ps = pubsub.write().await;

    for arg in args {
        if let Some(channel) = arg_to_string(arg) {
            let count = ps.subscribe(client.id, &channel, pubsub_tx.clone());
            client.subscriptions = count;
            responses.push(RespValue::array(vec![
                RespValue::bulk_string(b"subscribe".to_vec()),
                RespValue::bulk_string(channel.into_bytes()),
                RespValue::integer(count as i64),
            ]));
        }
    }

    // Concatenate all response bytes
    let mut buf = Vec::new();
    for resp in &responses {
        resp.write_to(&mut buf);
    }
    // Return the first response; remaining will be sent via pub/sub channel
    // Actually, for SUBSCRIBE we need to send all confirmations.
    // We'll send the first as the return value and push the rest into the sender.
    if responses.len() <= 1 {
        return responses.into_iter().next().unwrap_or(RespValue::ok());
    }

    let first = responses.remove(0);
    for resp in responses {
        let _ = pubsub_tx.send(resp);
    }
    first
}

pub async fn cmd_unsubscribe(
    args: &[RespValue],
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
) -> RespValue {
    let mut ps = pubsub.write().await;

    let channels: Vec<String> = if args.is_empty() {
        ps.client_channel_list(client.id)
    } else {
        args.iter().filter_map(arg_to_string).collect()
    };

    if channels.is_empty() {
        client.subscriptions = 0;
        return RespValue::array(vec![
            RespValue::bulk_string(b"unsubscribe".to_vec()),
            RespValue::null_bulk_string(),
            RespValue::integer(0),
        ]);
    }

    let mut responses = Vec::new();
    for channel in channels {
        let count = ps.unsubscribe(client.id, &channel);
        client.subscriptions = count;
        responses.push(RespValue::array(vec![
            RespValue::bulk_string(b"unsubscribe".to_vec()),
            RespValue::bulk_string(channel.into_bytes()),
            RespValue::integer(count as i64),
        ]));
    }

    let first = responses.remove(0);
    for resp in responses {
        let _ = pubsub_tx.send(resp);
    }
    first
}

pub async fn cmd_psubscribe(
    args: &[RespValue],
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("psubscribe");
    }

    let mut responses = Vec::new();
    let mut ps = pubsub.write().await;

    for arg in args {
        if let Some(pattern) = arg_to_string(arg) {
            let count = ps.psubscribe(client.id, &pattern, pubsub_tx.clone());
            client.subscriptions = count;
            responses.push(RespValue::array(vec![
                RespValue::bulk_string(b"psubscribe".to_vec()),
                RespValue::bulk_string(pattern.into_bytes()),
                RespValue::integer(count as i64),
            ]));
        }
    }

    if responses.len() <= 1 {
        return responses.into_iter().next().unwrap_or(RespValue::ok());
    }

    let first = responses.remove(0);
    for resp in responses {
        let _ = pubsub_tx.send(resp);
    }
    first
}

pub async fn cmd_punsubscribe(
    args: &[RespValue],
    client: &mut ClientState,
    pubsub: &SharedPubSub,
    pubsub_tx: &mpsc::UnboundedSender<RespValue>,
) -> RespValue {
    let mut ps = pubsub.write().await;

    let patterns: Vec<String> = if args.is_empty() {
        ps.client_pattern_list(client.id)
    } else {
        args.iter().filter_map(arg_to_string).collect()
    };

    if patterns.is_empty() {
        client.subscriptions = 0;
        return RespValue::array(vec![
            RespValue::bulk_string(b"punsubscribe".to_vec()),
            RespValue::null_bulk_string(),
            RespValue::integer(0),
        ]);
    }

    let mut responses = Vec::new();
    for pattern in patterns {
        let count = ps.punsubscribe(client.id, &pattern);
        client.subscriptions = count;
        responses.push(RespValue::array(vec![
            RespValue::bulk_string(b"punsubscribe".to_vec()),
            RespValue::bulk_string(pattern.into_bytes()),
            RespValue::integer(count as i64),
        ]));
    }

    let first = responses.remove(0);
    for resp in responses {
        let _ = pubsub_tx.send(resp);
    }
    first
}

pub async fn cmd_publish(args: &[RespValue], pubsub: &SharedPubSub) -> RespValue {
    if args.len() != 2 {
        return wrong_arg_count("publish");
    }
    let channel = match arg_to_string(&args[0]) {
        Some(c) => c,
        None => return RespValue::error("ERR invalid channel"),
    };
    let message = match arg_to_bytes(&args[1]) {
        Some(m) => m.to_vec(),
        None => return RespValue::error("ERR invalid message"),
    };

    let ps = pubsub.read().await;
    let delivered = ps.publish(&channel, &message);
    RespValue::integer(delivered as i64)
}

pub async fn cmd_pubsub(args: &[RespValue], pubsub: &SharedPubSub) -> RespValue {
    if args.is_empty() {
        return wrong_arg_count("pubsub");
    }

    let subcmd = match arg_to_string(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return RespValue::error("ERR invalid subcommand"),
    };

    let ps = pubsub.read().await;

    match subcmd.as_str() {
        "CHANNELS" => {
            let pattern = args.get(1).and_then(arg_to_string);
            let channels = ps.channels_matching(pattern.as_deref());
            let items: Vec<RespValue> = channels
                .into_iter()
                .map(|ch| RespValue::bulk_string(ch.into_bytes()))
                .collect();
            RespValue::array(items)
        }
        "NUMSUB" => {
            let channel_names: Vec<String> = args[1..].iter().filter_map(arg_to_string).collect();
            let results = ps.numsub(&channel_names);
            let mut items = Vec::new();
            for (name, count) in results {
                items.push(RespValue::bulk_string(name.into_bytes()));
                items.push(RespValue::integer(count as i64));
            }
            RespValue::array(items)
        }
        "NUMPAT" => RespValue::integer(ps.numpat() as i64),
        _ => RespValue::error(format!("ERR Unknown PUBSUB subcommand '{subcmd}'")),
    }
}
