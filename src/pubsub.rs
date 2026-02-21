use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use crate::glob::glob_match;
use crate::resp::RespValue;

pub type PubSubSender = mpsc::UnboundedSender<RespValue>;
pub type PubSubReceiver = mpsc::UnboundedReceiver<RespValue>;

/// Registry for Pub/Sub channel and pattern subscriptions.
pub struct PubSubRegistry {
    /// channel name -> set of subscribed client IDs
    channels: HashMap<String, HashSet<u64>>,
    /// pattern string -> set of subscribed client IDs
    patterns: HashMap<String, HashSet<u64>>,
    /// client_id -> sender for pushing messages to the client's connection
    senders: HashMap<u64, PubSubSender>,
    /// client_id -> set of channels subscribed
    client_channels: HashMap<u64, HashSet<String>>,
    /// client_id -> set of patterns subscribed
    client_patterns: HashMap<u64, HashSet<String>>,
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubRegistry {
    pub fn new() -> Self {
        PubSubRegistry {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            senders: HashMap::new(),
            client_channels: HashMap::new(),
            client_patterns: HashMap::new(),
        }
    }

    /// Subscribe a client to a channel. Returns the client's total subscription count.
    pub fn subscribe(&mut self, client_id: u64, channel: &str, sender: PubSubSender) -> usize {
        self.senders.entry(client_id).or_insert(sender);
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(client_id);
        self.client_channels
            .entry(client_id)
            .or_default()
            .insert(channel.to_string());
        self.subscription_count(client_id)
    }

    /// Unsubscribe a client from a channel. Returns the client's remaining subscription count.
    pub fn unsubscribe(&mut self, client_id: u64, channel: &str) -> usize {
        if let Some(clients) = self.channels.get_mut(channel) {
            clients.remove(&client_id);
            if clients.is_empty() {
                self.channels.remove(channel);
            }
        }
        if let Some(chans) = self.client_channels.get_mut(&client_id) {
            chans.remove(channel);
        }
        let count = self.subscription_count(client_id);
        if count == 0 {
            self.senders.remove(&client_id);
            self.client_channels.remove(&client_id);
        }
        count
    }

    /// Subscribe a client to a pattern. Returns the client's total subscription count.
    pub fn psubscribe(&mut self, client_id: u64, pattern: &str, sender: PubSubSender) -> usize {
        self.senders.entry(client_id).or_insert(sender);
        self.patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(client_id);
        self.client_patterns
            .entry(client_id)
            .or_default()
            .insert(pattern.to_string());
        self.subscription_count(client_id)
    }

    /// Unsubscribe a client from a pattern. Returns the client's remaining subscription count.
    pub fn punsubscribe(&mut self, client_id: u64, pattern: &str) -> usize {
        if let Some(clients) = self.patterns.get_mut(pattern) {
            clients.remove(&client_id);
            if clients.is_empty() {
                self.patterns.remove(pattern);
            }
        }
        if let Some(pats) = self.client_patterns.get_mut(&client_id) {
            pats.remove(pattern);
        }
        let count = self.subscription_count(client_id);
        if count == 0 {
            self.senders.remove(&client_id);
            self.client_patterns.remove(&client_id);
        }
        count
    }

    /// Publish a message to a channel. Returns the number of clients that received it.
    pub fn publish(&self, channel: &str, message: &[u8]) -> usize {
        let mut delivered = 0;

        // Direct channel subscribers
        if let Some(client_ids) = self.channels.get(channel) {
            for &client_id in client_ids {
                if let Some(sender) = self.senders.get(&client_id) {
                    let msg = RespValue::array(vec![
                        RespValue::bulk_string(b"message".to_vec()),
                        RespValue::bulk_string(channel.as_bytes().to_vec()),
                        RespValue::bulk_string(message.to_vec()),
                    ]);
                    if sender.send(msg).is_ok() {
                        delivered += 1;
                    }
                }
            }
        }

        // Pattern subscribers
        for (pattern, client_ids) in &self.patterns {
            if glob_match(pattern, channel) {
                for &client_id in client_ids {
                    if let Some(sender) = self.senders.get(&client_id) {
                        let msg = RespValue::array(vec![
                            RespValue::bulk_string(b"pmessage".to_vec()),
                            RespValue::bulk_string(pattern.as_bytes().to_vec()),
                            RespValue::bulk_string(channel.as_bytes().to_vec()),
                            RespValue::bulk_string(message.to_vec()),
                        ]);
                        if sender.send(msg).is_ok() {
                            delivered += 1;
                        }
                    }
                }
            }
        }

        delivered
    }

    /// Remove all subscriptions for a client (called on disconnect).
    pub fn unsubscribe_all(&mut self, client_id: u64) {
        if let Some(chans) = self.client_channels.remove(&client_id) {
            for channel in chans {
                if let Some(clients) = self.channels.get_mut(&channel) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.channels.remove(&channel);
                    }
                }
            }
        }
        if let Some(pats) = self.client_patterns.remove(&client_id) {
            for pattern in pats {
                if let Some(clients) = self.patterns.get_mut(&pattern) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.patterns.remove(&pattern);
                    }
                }
            }
        }
        self.senders.remove(&client_id);
    }

    /// List channels matching a pattern (for PUBSUB CHANNELS).
    pub fn channels_matching(&self, pattern: Option<&str>) -> Vec<String> {
        match pattern {
            Some(pat) => self
                .channels
                .keys()
                .filter(|ch| glob_match(pat, ch))
                .cloned()
                .collect(),
            None => self.channels.keys().cloned().collect(),
        }
    }

    /// Get subscriber count for channels (for PUBSUB NUMSUB).
    pub fn numsub(&self, channel_names: &[String]) -> Vec<(String, usize)> {
        channel_names
            .iter()
            .map(|ch| {
                let count = self.channels.get(ch).map_or(0, |s| s.len());
                (ch.clone(), count)
            })
            .collect()
    }

    /// Get the number of active pattern subscriptions (for PUBSUB NUMPAT).
    pub fn numpat(&self) -> usize {
        self.patterns.values().map(|s| s.len()).sum()
    }

    /// Get the channels a specific client is subscribed to.
    pub fn client_channel_list(&self, client_id: u64) -> Vec<String> {
        self.client_channels
            .get(&client_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the patterns a specific client is subscribed to.
    pub fn client_pattern_list(&self, client_id: u64) -> Vec<String> {
        self.client_patterns
            .get(&client_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn subscription_count(&self, client_id: u64) -> usize {
        let chans = self.client_channels.get(&client_id).map_or(0, |s| s.len());
        let pats = self.client_patterns.get(&client_id).map_or(0, |s| s.len());
        chans + pats
    }
}

pub type SharedPubSub = Arc<RwLock<PubSubRegistry>>;
