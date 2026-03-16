use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct Broker {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    patterns: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub pattern: Option<String>,
    pub payload: String,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.channels.write().await;
        let tx = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub async fn psubscribe(&self, pattern: &str) -> broadcast::Receiver<Message> {
        let mut patterns = self.patterns.write().await;
        let tx = patterns
            .entry(pattern.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub async fn publish(&self, channel: &str, payload: String) -> i64 {
        let mut count = 0;

        // Direct channel subscribers
        {
            let channels = self.channels.read().await;
            if let Some(tx) = channels.get(channel) {
                let msg = Message {
                    channel: channel.to_string(),
                    pattern: None,
                    payload: payload.clone(),
                };
                count += tx.send(msg).unwrap_or(0) as i64;
            }
        }

        // Pattern subscribers
        {
            let patterns = self.patterns.read().await;
            for (pattern, tx) in patterns.iter() {
                if glob_match(pattern, channel) {
                    let msg = Message {
                        channel: channel.to_string(),
                        pattern: Some(pattern.clone()),
                        payload: payload.clone(),
                    };
                    count += tx.send(msg).unwrap_or(0) as i64;
                }
            }
        }

        count
    }
}

pub fn glob_match(pattern: &str, s: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = s.chars().collect();
    do_glob_match(&p, &s, 0, 0)
}

fn do_glob_match(pattern: &[char], s: &[char], pi: usize, si: usize) -> bool {
    if pi == pattern.len() && si == s.len() {
        return true;
    }
    if pi == pattern.len() {
        return false;
    }
    if pattern[pi] == '*' {
        for i in si..=s.len() {
            if do_glob_match(pattern, s, pi + 1, i) {
                return true;
            }
        }
        return false;
    }
    if si == s.len() {
        return false;
    }
    if pattern[pi] == '?' || pattern[pi] == s[si] {
        return do_glob_match(pattern, s, pi + 1, si + 1);
    }
    false
}
