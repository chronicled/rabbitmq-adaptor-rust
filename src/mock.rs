use super::{ConsumerResult, ConsumerStream, RabbitExt};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use lapin::message::Delivery;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedMessage {
    pub exchange: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

#[derive(Default, Clone)]
pub struct MockRabbitConnection {
    pub published_messages: Arc<RwLock<Vec<PublishedMessage>>>,
}

impl MockRabbitConnection {
    pub fn get_published_messages(&self) -> Vec<PublishedMessage> {
        self.published_messages.read().unwrap().clone()
    }
}

#[async_trait]
impl RabbitExt for MockRabbitConnection {
    async fn create_topic_exchange(&self, _name: String) -> Result<()> {
        Ok(())
    }

    async fn create_queue(&self, _name: String) -> Result<()> {
        Ok(())
    }

    async fn bind_queue(
        &self,
        _name: String,
        _exchange: String,
        _routing_key: String,
    ) -> Result<()> {
        Ok(())
    }

    async fn register_consumer<
        'a,
        F: 'static + FnMut(Delivery) -> BoxFuture<'a, Result<ConsumerResult>> + Send + Sync + Clone,
    >(
        self,
        _name: String,
        _prefetch_count: Option<u16>,
        _callback: F,
    ) -> Result<()> {
        Ok(())
    }

    fn create_consumer_stream(self, _name: String, _prefetch_count: Option<u16>) -> ConsumerStream {
        unimplemented!("MockRabbitConnection does not support consumer streams")
    }

    async fn publish(
        self,
        exchange: String,
        topic: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<()> {
        self.published_messages
            .write()
            .unwrap()
            .push(PublishedMessage {
                exchange,
                topic,
                payload,
                headers,
            });
        Ok(())
    }

    async fn rpc_request(
        self,
        _exchange: String,
        _topic: String,
        _payload: Vec<u8>,
        _headers: Vec<(String, String)>,
    ) -> Result<Delivery> {
        unimplemented!("MockRabbitConnection does not support rpc")
    }

    async fn rpc_response(
        self,
        _message: &Delivery,
        _payload: Vec<u8>,
        _headers: Vec<(String, String)>,
    ) -> Result<()> {
        unimplemented!("MockRabbitConnection does not support rpc")
    }

    async fn close_default_channel(&self) -> Result<()> {
        Ok(())
    }
}
