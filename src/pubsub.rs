use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct Broker {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: String,
    pub payload: String,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.channels.write().await;
        let tx = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    pub async fn publish(&self, channel: &str, payload: String) -> i64 {
        let channels = self.channels.read().await;
        if let Some(tx) = channels.get(channel) {
            let msg = Message {
                channel: channel.to_string(),
                payload,
            };
            tx.send(msg).unwrap_or(0) as i64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_and_publish() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("test-channel").await;
        let count = broker.publish("test-channel", "hello".to_string()).await;
        assert_eq!(count, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "test-channel");
        assert_eq!(msg.payload, "hello");
    }

    #[tokio::test]
    async fn publish_to_empty_channel_returns_zero() {
        let broker = Broker::new();
        let count = broker
            .publish("nobody-listening", "hello".to_string())
            .await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let broker = Broker::new();
        let mut rx1 = broker.subscribe("ch").await;
        let mut rx2 = broker.subscribe("ch").await;
        broker.publish("ch", "msg".to_string()).await;
        assert_eq!(rx1.recv().await.unwrap().payload, "msg");
        assert_eq!(rx2.recv().await.unwrap().payload, "msg");
    }

    #[tokio::test]
    async fn separate_channels_are_isolated() {
        let broker = Broker::new();
        let mut rx_a = broker.subscribe("a").await;
        let _rx_b = broker.subscribe("b").await;
        broker.publish("a", "only-a".to_string()).await;
        assert_eq!(rx_a.recv().await.unwrap().payload, "only-a");
    }

    #[tokio::test]
    async fn multiple_messages_in_order() {
        let broker = Broker::new();
        let mut rx = broker.subscribe("ch").await;
        broker.publish("ch", "first".to_string()).await;
        broker.publish("ch", "second".to_string()).await;
        broker.publish("ch", "third".to_string()).await;
        assert_eq!(rx.recv().await.unwrap().payload, "first");
        assert_eq!(rx.recv().await.unwrap().payload, "second");
        assert_eq!(rx.recv().await.unwrap().payload, "third");
    }
}
