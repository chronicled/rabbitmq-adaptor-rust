use super::*;
use anyhow::anyhow;
use derivative::Derivative;
use futures::future::err;
use futures::ready;
use futures::task::{Context, Poll};
use lapin::Consumer;
use std::pin::Pin;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ConsumerStream {
    #[derivative(Debug = "ignore")]
    rabbit: RabbitConnection,
    queue: String,
    host: String,
    prefetch_count: u16,
    #[derivative(Debug = "ignore")]
    consumer_channel: Option<(Consumer, Channel)>,
    #[derivative(Debug = "ignore")]
    acquire_consumer_channel: BoxFuture<'static, (Consumer, Channel)>,
}

impl ConsumerStream {
    pub fn new(rabbit: RabbitConnection, queue: String, prefetch_count: Option<u16>) -> Self {
        let host = rabbit.0.read().unwrap().host.clone();
        let prefetch_count = prefetch_count.unwrap_or(DEFAULT_PREFETCH_COUNT);
        let acquire_consumer_channel = Box::pin(acquire_consumer_channel(
            rabbit.clone(),
            queue.clone(),
            prefetch_count,
        ));
        Self {
            rabbit,
            host,
            queue,
            prefetch_count,
            consumer_channel: None,
            acquire_consumer_channel,
        }
    }

    pub fn try_acquire_consumer(&self) -> Option<Consumer> {
        self.consumer_channel.as_ref().map(|c| c.0.clone())
    }

    pub fn try_acquire_channel(&self) -> Option<Channel> {
        self.consumer_channel.as_ref().map(|c| c.1.clone())
    }

    #[instrument(skip(self))]
    pub async fn acquire_consumer(&mut self) -> Consumer {
        if let Some((consumer, _)) = &self.consumer_channel {
            consumer.clone()
        } else {
            let (consumer, channel) = acquire_consumer_channel(
                self.rabbit.clone(),
                self.queue.clone(),
                self.prefetch_count,
            )
            .await;
            if let Some((consumer, _)) = &self.consumer_channel {
                consumer.clone()
            } else {
                self.consumer_channel = Some((consumer.clone(), channel));
                consumer
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn acquire_channel(&mut self) -> Channel {
        if let Some((_, channel)) = &self.consumer_channel {
            channel.clone()
        } else {
            let (consumer, channel) = acquire_consumer_channel(
                self.rabbit.clone(),
                self.queue.clone(),
                self.prefetch_count,
            )
            .await;
            if let Some((_, channel)) = &self.consumer_channel {
                channel.clone()
            } else {
                self.consumer_channel = Some((consumer, channel.clone()));
                channel
            }
        }
    }

    #[instrument(skip(self))]
    pub fn ack(&self, delivery_tag: u64) -> BoxFuture<'static, Result<(), Error>> {
        if self.consumer_channel.is_none() {
            return Box::pin(err(anyhow!("Channel to ack does not exist.")));
        }
        let channel = if let Some(consumer_channel) = &self.consumer_channel {
            &consumer_channel.1
        } else {
            unreachable!("Channel is already set");
        };
        channel
            .basic_ack(delivery_tag, BasicAckOptions { multiple: false })
            .map_err(|error| error.into())
            .boxed()
    }

    #[instrument(skip(self))]
    pub fn nack(&self, delivery_tag: u64, requeue: bool) -> BoxFuture<'static, Result<(), Error>> {
        if self.consumer_channel.is_none() {
            return Box::pin(err(anyhow!("Channel to nack does not exist.")));
        }
        let channel = if let Some(consumer_channel) = &self.consumer_channel {
            &consumer_channel.1
        } else {
            unreachable!("Channel is already set");
        };
        channel
            .basic_nack(
                delivery_tag,
                BasicNackOptions {
                    requeue,
                    multiple: false,
                },
            )
            .map_err(|error| error.into())
            .boxed()
    }

    #[instrument(skip(self, cx))]
    fn retry_connection(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Delivery>> {
        error!(
            host = self.host,
            vhost = self.rabbit.0.read().unwrap().uri.vhost,
            queue = self.queue,
            "ConsumerStream disconnected"
        );

        self.consumer_channel = None;
        self.acquire_consumer_channel = Box::pin(acquire_consumer_channel(
            self.rabbit.clone(),
            self.queue.clone(),
            self.prefetch_count,
        ));
        let consumer_channel = ready!(self.acquire_consumer_channel.poll_unpin(cx));
        self.consumer_channel = Some(consumer_channel);
        Poll::Pending
    }
}

impl Stream for ConsumerStream {
    type Item = Delivery;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.consumer_channel.is_none() {
            let consumer_channel = ready!(self.acquire_consumer_channel.poll_unpin(cx));
            self.consumer_channel = Some(consumer_channel);
        }

        let consumer = if let Some(consumer_channel) = &mut self.consumer_channel {
            // check channel state, retry to get connection
            if !is_connected(&consumer_channel.1) {
                return self.retry_connection(cx);
            }
            &mut consumer_channel.0
        } else {
            unreachable!("Consumer is already set by ConsumerStream");
        };

        match consumer.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(message)),
            _ => self.retry_connection(cx),
        }
    }
}

#[instrument(skip(rabbit), fields(host, vhost))]
async fn acquire_consumer_channel(
    rabbit: RabbitConnection,
    queue: String,
    prefetch_count: u16,
) -> (Consumer, Channel) {
    rabbit.record_host_and_vhost_in_span();
    loop {
        let client = rabbit.get_connection().await;

        let channel = match create_channel(client).await {
            Ok(channel) => channel,
            Err(error) => {
                error!(?error, "Failed to get channel for consumer stream");
                continue;
            }
        };

        if let Err(error) = channel
            .basic_qos(prefetch_count, BasicQosOptions::default())
            .await
        {
            error!(?error, "Failed basic_qos for consumer stream");
            continue;
        }
        let rabbit_queue = Queue::new(queue.clone().into(), 0, 0);
        channel.register_queue(QueueState::from(rabbit_queue.clone()));
        if let Ok(consumer) = channel
            .basic_consume(
                &rabbit_queue,
                &Uuid::new_v4().to_string(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            info!("ConsumerStream connected");
            return (consumer, channel);
        }
    }
}

pub fn is_connected(channel: &Channel) -> bool {
    channel.status().is_connected()
}
