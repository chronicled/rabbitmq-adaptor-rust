use anyhow::{anyhow, Error};
use async_trait::async_trait;
use derivative::Derivative;
use enclose::enclose;
use futures::{channel::oneshot, future::BoxFuture, prelude::*};
pub use lapin::message::Delivery;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    protocol::{basic::AMQPProperties, BasicProperties},
    queue::{Queue, QueueState},
    types::{AMQPValue, FieldTable, ShortString},
    uri::{AMQPAuthority, AMQPQueryString, AMQPScheme, AMQPUri, AMQPUserInfo},
    Channel, Connection, ConnectionProperties, ExchangeKind,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    select,
    sync::{watch, Notify},
    time::{sleep, timeout},
};
use tracing::{debug, error, field, info, instrument, trace, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

mod consumer_stream;
pub mod display;

pub use consumer_stream::*;

mod mock;
mod tracing_propagation;
mod tracing_utilities;
pub use mock::MockRabbitConnection;
pub use mock::PublishedMessage;

use crate::{
    display::delivery::{DisplayData, DisplayDelivery, DisplayDeliveryOption},
    tracing_propagation::AmqpClientCarrier,
    tracing_utilities::{create_consumer_span_from_message, record_chronicled_ids_in_span},
};

const DEFAULT_PREFETCH_COUNT: u16 = 5;
const DEFAULT_RETRY_INTERVAL_SECONDS: u64 = 1;
const CHANNEL_TIMEOUT: u64 = 60000;
pub static PPS_PRESIGNED_PRMITIVE_TOPIC: &str = "protocol.out.primitive.presigned-transaction";

tokio::task_local! {
  pub static MESSAGE: Option<Delivery>;
}

const CORRELATION_ID_HEADER_KEY: &str = "correlation-id";
const CORRELATION_ID_TRACING_KEY: &str = "correlation_id";
const CAUSATION_ID_HEADER_KEY: &str = "causation-id";
const CAUSATION_ID_TRACING_KEY: &str = "causation_id";

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerResult {
    ACK,
    NACK(bool),
}

/// Struct which manages the RabbitMQ connection
///
/// Example usage:
/// ```no_run
/// use anyhow::Error;
/// use futures::prelude::*;
/// use tracing::{info, debug};
/// use rabbitmq_adaptor::{RabbitConnection, ConsumerResult, ConfigUri, RabbitExt};
/// use tokio;
///
/// tokio::spawn(async move {
///     let rabbit = RabbitConnection::new(
///     ConfigUri::Uri("amqp://guest:guest@127.0.0.1:5672/%2f".to_string()),
///         None,
///         "reply".to_string()
///     ).await.unwrap();
///     rabbit.create_topic_exchange("exchange".to_string()).await;
///     rabbit.create_queue("queue".to_string()).await;
///     rabbit.bind_queue("queue".to_string(), "exchange".to_string(), "#".to_string()).await;
///     let connection = rabbit.clone();
///     tokio::spawn(
///        rabbit
///            .clone()
///            .register_consumer("queue".to_string(), None, move |message| {
///                debug!("Got message {:?}", message);
///                let connection2 = rabbit.clone();
///                async move {
///                    connection2
///                        .publish("exchange".to_string(), "queue2".to_string(), b"payload".to_vec(), vec![])
///                        .await?;
///                    Ok(ConsumerResult::ACK)
///                }
///                .boxed()
///            }),
///    );
/// });
/// ```

#[derive(Clone)]
pub struct RabbitConnection(Arc<RwLock<RabbitConnectionInner>>);

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RabbitConnectionInner {
    uri: AMQPUri,
    host: String,
    #[derivative(Debug = "ignore")]
    connection_properties: ConnectionProperties,
    #[derivative(Debug = "ignore")]
    reply_queue: String,
    #[derivative(Debug = "ignore")]
    pending_replies: HashMap<String, oneshot::Sender<Delivery>>,
    #[derivative(Debug = "ignore")]
    connection_receiver: watch::Receiver<Option<(Connection, Channel)>>,
}

#[derive(Clone, Deserialize, Debug)]
#[serde(untagged)]
pub enum ConfigUri {
    Detailed {
        username: Option<String>,
        password: Option<String>,
        host: String,
        vhost: Option<String>,
        port: u16,
        frame_max: Option<u32>,
        heartbeat: Option<u16>,
    },
    Uri(String),
}

impl ConfigUri {
    pub fn get_lapin_uri(&self) -> AMQPUri {
        match self {
            ConfigUri::Detailed {
                username,
                password,
                host,
                vhost,
                port,
                frame_max,
                heartbeat,
            } => AMQPUri {
                scheme: AMQPScheme::AMQP,
                authority: AMQPAuthority {
                    userinfo: AMQPUserInfo {
                        username: username.as_ref().unwrap_or(&"guest".to_string()).clone(),
                        password: password.as_ref().unwrap_or(&"guest".to_string()).clone(),
                    },
                    host: host.clone(),
                    port: *port,
                },
                vhost: vhost.as_ref().unwrap_or(&"/".to_string()).to_string(),
                query: AMQPQueryString {
                    frame_max: *frame_max,
                    heartbeat: *heartbeat,
                    ..Default::default()
                },
            },
            ConfigUri::Uri(uri) => AMQPUri::from_str(uri).unwrap(),
        }
    }
}

impl Default for RabbitConnection {
    fn default() -> RabbitConnection {
        RabbitConnection(Arc::new(RwLock::new(RabbitConnectionInner {
            uri: "amqp://127.0.0.1:5672/%2f".parse().unwrap(),
            host: "127.0.0.1:5672".to_string(),
            connection_properties: ConnectionProperties::default(),
            reply_queue: "".to_string(),
            pending_replies: HashMap::new(),
            connection_receiver: watch::channel(None).1,
        })))
    }
}

impl RabbitConnection {
    #[instrument]
    pub async fn new(
        config_uri: ConfigUri,
        con_properties: Option<ConnectionProperties>,
        reply_queue: String,
    ) -> Result<RabbitConnection, Error> {
        let uri: AMQPUri = config_uri.get_lapin_uri();
        let host = format!("{}:{}", uri.authority.host, uri.authority.port);
        let properties = match con_properties {
            Some(properties) => properties,
            None => ConnectionProperties::default(),
        };
        let (notifier, receiver) = watch::channel(None);
        let conn = RabbitConnection(Arc::new(RwLock::new(RabbitConnectionInner {
            uri,
            host,
            connection_properties: properties,
            reply_queue,
            pending_replies: HashMap::new(),
            connection_receiver: receiver,
        })));
        tokio::spawn(
            enclose!((conn) async move {conn.connect_loop(notifier).in_current_span().await}),
        );
        Ok(conn)
    }

    #[instrument(skip(self, notifier), fields(host, vhost))]
    async fn connect_loop(&self, notifier: watch::Sender<Option<(Connection, Channel)>>) {
        self.record_host_and_vhost_in_span();
        loop {
            notifier.send(None).unwrap();

            info!("Connecting to RabbitMQ");

            let (amqp_uri, properties, reply_queue) = {
                let inner = self.0.read().unwrap();
                (
                    inner.uri.clone(),
                    inner.connection_properties.clone(),
                    inner.reply_queue.clone(),
                )
            };

            let mut retry_count = 0;
            let (client, channel) = loop {
                let amqp_uri = amqp_uri.clone();
                let properties = properties.clone();

                let connect_and_channel_future = async move {
                    let client = connect_future(amqp_uri, properties).await?;
                    let channel = create_channel(client.clone()).await?;
                    <Result<_, lapin::Error>>::Ok((client, channel))
                };

                match timeout(Duration::from_secs(60), connect_and_channel_future).await {
                    Ok(Ok(client_channel)) => break client_channel,
                    Ok(Err(error)) => {
                        sleep(Duration::from_secs(DEFAULT_RETRY_INTERVAL_SECONDS)).await;
                        retry_count += 1;
                        error!(?error, "RabbitMQ connection failed");
                        info!(retry_count, "Retrying RabbitMQ connection");
                    }
                    Err(_) => {
                        retry_count += 1;
                        error!("RabbitMQ connection timed out");
                        info!(retry_count, "Retrying RabbitMQ connection",);
                    }
                }
            };

            let error_notifier = Arc::new(Notify::new());
            let error_notified = error_notifier.clone();

            client.on_error(Box::new(move || {
                error_notifier.notify_one();
            }));

            // notify to all the receivers that connection is done
            notifier.send(Some((client, channel))).unwrap();

            self.clone().setup_reply_queue(reply_queue);

            info!("RabbitMQ Adaptor connected and configured");

            // wait for Connection to call notify() on error
            error_notified.notified().await;

            info!("RabbitMQ connection closed");
        }
    }

    pub async fn get_connection(&self) -> Connection {
        let mut receiver = self.0.read().unwrap().connection_receiver.clone();
        loop {
            if receiver.changed().await.is_ok() {
                if let Some((connection, _)) = receiver.borrow().clone() {
                    if connection.status().connected() {
                        return connection;
                    }
                }
            }
        }
    }

    pub async fn get_default_channel(&self) -> Channel {
        let mut receiver = self.0.read().unwrap().connection_receiver.clone();
        loop {
            if receiver.changed().await.is_ok() {
                if let Some((_, channel)) = receiver.borrow().clone() {
                    if channel.status().is_connected() {
                        return channel;
                    }
                }
            }
        }
    }

    #[instrument(skip(self))]
    fn setup_reply_queue(self, reply_queue: String) {
        debug!("Setting up reply queue listener",);
        tokio::spawn(self.clone().register_consumer(
            reply_queue,
            None,
            enclose!((self => con) move |message| {
                enclose!((con) async move { con.reply_queue_callback(&message) }.boxed())
            }),
        ));
    }

    #[instrument(skip(self), fields(host, vhost))]
    fn reply_queue_callback(&self, message: &Delivery) -> Result<ConsumerResult, Error> {
        self.record_host_and_vhost_in_span();
        match message.properties.correlation_id() {
            Some(correlation_id) => {
                let sender = {
                    self.0
                        .write()
                        .unwrap()
                        .pending_replies
                        .remove(&correlation_id.to_string())
                };
                match sender {
                    Some(sender) => {
                        sender.send(message.clone()).unwrap();
                        trace!("Sending");
                    }
                    None => error!("Ignoring message with unknown correlation_id on reply queue"),
                }
            }
            None => error!("Ignoring message without correlation_id on reply queue"),
        };
        Ok(ConsumerResult::ACK)
    }

    async fn do_consume<
        'a,
        F: 'static
            + FnMut(Delivery) -> BoxFuture<'a, Result<ConsumerResult, Error>>
            + Send
            + Sync
            + Clone,
    >(
        &self,
        queue: String,
        prefetch_count: Option<u16>,
        callback: &F,
    ) -> Result<(), lapin::Error> {
        let client = self.get_connection().await;

        let channel = create_channel(client).await?;

        let prefetch_count = prefetch_count.unwrap_or(DEFAULT_PREFETCH_COUNT);
        channel
            .basic_qos(prefetch_count, BasicQosOptions::default())
            .await?;
        let queue = Queue::new(queue.clone().into(), 0, 0);
        channel.register_queue(QueueState::from(queue.clone()));
        let mut stream = channel
            .basic_consume(
                &queue,
                &Uuid::new_v4().to_string(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        while let Some(message) = stream.next().await {
            let message = message?;
            let span = create_consumer_span_from_message(&message);

            tokio::spawn(
                enclose!((channel, mut callback) MESSAGE.scope(Some(message.clone()), async move {
                    let Delivery {
                        delivery_tag,
                        routing_key,
                        data,
                        ..
                    } = &message;

                    if routing_key.as_str().starts_with(PPS_PRESIGNED_PRMITIVE_TOPIC) {
                        debug!("Received presigned primitive message");
                        trace!(data = %DisplayData(data), "Consumer message ready");
                    } else {
                        trace!(data = %DisplayData(data), "Consumer message ready");
                    }

                    match callback(message.clone()).await {
                        Ok(ConsumerResult::ACK) => {
                            trace!(delivery_tag, "Consumer acked message");
                            let ack_result = channel
                                .basic_ack(*delivery_tag, BasicAckOptions { multiple: false })
                                .await;
                            if let Err(error) = ack_result {
                                error!(?error, "Consumer ACK failed");
                            }
                        }
                        Ok(ConsumerResult::NACK(requeue)) => {
                            trace!(delivery_tag, requeue,
                                "Consumer nacked message"
                            );
                            if !requeue {
                                MESSAGE.try_with(|message| warn!(
                                    data = %DisplayDeliveryOption(message.as_ref().map(DisplayDelivery)),
                                    "Message will be deleted!  Use data key for manual recovery")
                                ).expect("There should be a message task-local");
                            }
                            let nack_result = channel
                                .basic_nack(
                                    *delivery_tag,
                                    BasicNackOptions {
                                        requeue,
                                        multiple: false,
                                    },
                                )
                                .await;
                            if let Err(error) = nack_result {
                                error!(?error, "Consumer failed NACK");
                            }
                        }
                        Err(error) => {
                            MESSAGE.try_with(|message| error!(
                                delivery_tag,
                                ?error,
                                data = %DisplayDeliveryOption(message.as_ref().map(DisplayDelivery)),
                                "Consumer returned an error, message will be deleted! Use data key for manual recovery.")
                            ).expect("Message task local key should exist");
                            let nack_result = channel
                                .basic_nack(
                                    *delivery_tag,
                                    BasicNackOptions {
                                        requeue: false,
                                        multiple: false,
                                    },
                                )
                                .await;
                            if let Err(error) = nack_result {
                                error!(?error, "Consumer failed NACK");
                            }
                        }
                    }
                }.instrument(span))),
            );
        }

        let channel_state = channel.status().state();

        if let lapin::ChannelState::Error = channel_state {
            Err(lapin::Error::InvalidChannelState(channel_state))
        } else {
            Ok(())
        }
    }

    fn record_host_and_vhost_in_span(&self) {
        let this = self.0.read().unwrap();
        Span::current().record("host", field::display(&this.host));
        Span::current().record("vhost", field::display(&this.uri.vhost));
    }
}

/// RabbitExt overloads the futures generated from the connect() method to allow for easy chaining
/// of futures.
#[async_trait]
pub trait RabbitExt {
    /// Creates a topic exchange
    /// https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-topic
    async fn create_topic_exchange(&self, name: String) -> Result<(), Error>;

    /// Creates a queue
    /// https://www.rabbitmq.com/tutorials/amqp-concepts.html#queues
    async fn create_queue(&self, name: String) -> Result<(), Error>;

    /// Binds a queue to a topic exchange
    /// https://www.rabbitmq.com/tutorials/amqp-concepts.html#bindings
    async fn bind_queue(
        &self,
        name: String,
        exchange: String,
        routing_key: String,
    ) -> Result<(), Error>;

    /// Register a push consumer, supplying a callback which is executed on each incoming message
    /// The callback function should return true for ack and false for nack
    /// https://www.rabbitmq.com/tutorials/amqp-concepts.html#consumers
    async fn register_consumer<
        'a,
        F: 'static
            + FnMut(Delivery) -> BoxFuture<'a, Result<ConsumerResult, Error>>
            + Send
            + Sync
            + Clone,
    >(
        self,
        queue: String,
        prefetch_count: Option<u16>,
        callback: F,
    ) -> Result<(), Error>;

    /// Returns a ConsumerStream that is connection resilient.
    /// The stream does not ack/nack for messages,
    /// so you should either call ack or nack method on the stream.
    fn create_consumer_stream(self, queue: String, prefetch_count: Option<u16>) -> ConsumerStream;

    /// Publish a message to a topic exchange
    async fn publish(
        self,
        exchange: String,
        topic: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<(), Error>;

    /// Publish an RPC message to a topic exchange
    async fn rpc_request(
        self,
        exchange: String,
        topic: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<Delivery, Error>;

    /// Publish an RPC response. This function will set the correlation_id and reply_to fields.
    async fn rpc_response(
        self,
        message: &Delivery,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<(), Error>;

    /// Close the channel gracefully
    async fn close_default_channel(&self) -> Result<(), Error>;
}

#[async_trait]
impl RabbitExt for RabbitConnection {
    #[instrument(skip(self), fields(host, vhost))]
    async fn create_topic_exchange(&self, exchange: String) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        loop {
            let channel = self.get_default_channel().await;
            let res = channel
                .exchange_declare(
                    &exchange,
                    ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await;
            if let Err(error) = res {
                error!(?error, "Error creating topic_exchange");
            } else {
                break;
            }
        }
        debug!("Created exchange");
        Ok(())
    }

    #[instrument(skip(self), fields(host, vhost))]
    async fn create_queue(&self, queue: String) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        loop {
            let channel = self.get_default_channel().await;
            let res = channel
                .queue_declare(
                    &queue,
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await;
            if let Err(error) = res {
                error!(?error, "Error creating queue");
            } else {
                break;
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(host, vhost))]
    async fn bind_queue(
        &self,
        queue: String,
        exchange: String,
        routing_key: String,
    ) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        loop {
            let channel = self.get_default_channel().await;
            let res = channel
                .queue_bind(
                    &queue,
                    &exchange,
                    &routing_key,
                    QueueBindOptions {
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await;
            if let Err(error) = res {
                error!(?error, "Error binding queue");
            } else {
                break;
            }
        }
        debug!("Created binding");
        Ok(())
    }

    // Blocks a dedicated channel and registers a consumer.
    // The callback will be called for incoming messages.
    // If the callback returns true, basic.ack will be sent in response.
    // If the callback returns false, basic.nack will be sent in response.
    #[instrument(level = "debug", skip(callback, self), fields(host, vhost))]
    async fn register_consumer<
        'a,
        F: 'static
            + FnMut(Delivery) -> BoxFuture<'a, Result<ConsumerResult, Error>>
            + Send
            + Sync
            + Clone,
    >(
        self,
        queue: String,
        prefetch_count: Option<u16>,
        callback: F,
    ) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        loop {
            if let Err(error) = self
                .do_consume(queue.clone(), prefetch_count, &callback)
                .await
            {
                error!(?error, "Error registering consumer");
            }
        }
    }

    #[instrument(skip(self), fields(host, vhost))]
    fn create_consumer_stream(self, queue: String, prefetch_count: Option<u16>) -> ConsumerStream {
        self.record_host_and_vhost_in_span();
        ConsumerStream::new(self, queue, prefetch_count)
    }

    #[instrument(
        fields(host, vhost, correlation_id, causation_id),
        skip(self, payload, headers)
    )]
    async fn publish(
        self,
        exchange: String,
        topic: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        let mut headers: BTreeMap<String, String> = headers.into_iter().collect();

        add_correlation_id(&mut headers);
        add_causation_id(&mut headers);
        record_chronicled_ids_in_span(&headers);
        let span = Span::current();
        // retrieve the current context
        let cx = span.context();
        // inject the current context through the amqp headers
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut AmqpClientCarrier::new(&mut headers))
        });
        if let Ok(payload) = std::str::from_utf8(&payload) {
            trace!(payload, ?headers, "Sending Message");
        } else {
            trace!(?headers, "Sending Message");
        }

        loop {
            let channel = self.get_default_channel().await;

            let res = select! {
                _ = sleep(Duration::from_millis(CHANNEL_TIMEOUT)) => {
                    error!("Timeout while publishing");
                    Err(lapin::Error::UnexpectedReply)
                },
                res = channel.basic_publish_and_confirm(
                    &exchange,
                    &topic,
                    BasicPublishOptions::default(),
                    payload.clone(),
                    headers_to_basic_properties(&headers),
                ).in_current_span() => res
            };
            match res {
                Err(lapin::Error::UnexpectedReply) => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    error!(?error, "Error while publishing");
                }
                Ok(_) => break,
            }
        }
        debug!("Published and confirmed");
        Ok(())
    }

    #[instrument(fields(host, vhost, correlation_id, causation_id), skip(self, payload))]
    async fn rpc_request(
        self,
        exchange: String,
        topic: String,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<Delivery, Error> {
        self.record_host_and_vhost_in_span();
        let mut headers: BTreeMap<String, String> = headers.into_iter().collect();
        let correlation_id_rpc = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel::<Delivery>();
        let reply_queue = {
            let mut con_lock = self.0.write().unwrap();
            con_lock
                .pending_replies
                .insert(correlation_id_rpc.clone(), sender);
            con_lock.reply_queue.clone()
        };
        add_correlation_id(&mut headers);
        add_causation_id(&mut headers);
        record_chronicled_ids_in_span(&headers);
        debug!("Sending RPC Request");
        loop {
            let channel = self.get_default_channel().await;
            let res = select! {
                _ = sleep(Duration::from_millis(CHANNEL_TIMEOUT)) => {
                    error!("Timeout during rpc_request");
                    Err(lapin::Error::UnexpectedReply)
                }
                res = channel.basic_publish_and_confirm(
                    &exchange,
                    &topic,
                    BasicPublishOptions::default(),
                    payload.clone(),
                    headers_to_basic_properties(&headers)
                        .with_correlation_id(correlation_id_rpc.clone().into())
                        .with_reply_to(reply_queue.clone().into()),
                ) => res
            };
            match res {
                Err(lapin::Error::UnexpectedReply) => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    error!(?error, "Error during rpc_request");
                }
                Ok(_) => break,
            }
        }
        debug!("RPC Request confirmed, waiting for delivery");
        let delivery = receiver.await?;
        Ok(delivery)
    }

    #[instrument(
        fields(host, vhost, correlation_id, causation_id),
        skip(self, payload, message)
    )]
    async fn rpc_response(
        self,
        message: &Delivery,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<(), Error> {
        self.record_host_and_vhost_in_span();
        let mut headers: BTreeMap<String, String> = headers.into_iter().collect();
        add_correlation_id(&mut headers);
        add_causation_id(&mut headers);
        record_chronicled_ids_in_span(&headers);
        debug!("Sending RPC Response");
        match (
            message.properties.reply_to().clone(),
            message.properties.correlation_id().clone(),
        ) {
            (Some(reply_to), Some(correlation_id_rpc)) => {
                let reply_to = &reply_to.to_string();
                loop {
                    let channel = self.get_default_channel().await;
                    let res = select! {
                        _ = sleep(Duration::from_millis(CHANNEL_TIMEOUT)) => {
                            error!("Timeout during rpc_response");
                            Err(lapin::Error::UnexpectedReply)
                        }
                        res = channel.basic_publish_and_confirm(
                            "",
                            reply_to,
                            BasicPublishOptions::default(),
                            payload.clone(),
                            headers_to_basic_properties(&headers)
                                .with_correlation_id(correlation_id_rpc.clone()),
                        ) => res
                    };
                    match res {
                        Err(lapin::Error::UnexpectedReply) => {
                            sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => {
                            error!(?error, "Error during rpc_response");
                        }
                        Ok(_) => break,
                    }
                }
                Ok(())
            }
            _ => Err(anyhow!(
                "Cannot send RPC reply for a message without reply_to or correlation_id field.",
            )),
        }
    }

    #[instrument(skip(self))]
    async fn close_default_channel(&self) -> Result<(), Error> {
        let channel = self.get_default_channel().await;
        channel.close(200, "Bye").await?;
        Ok(())
    }
}

pub trait DeliveryExt {
    fn get_header(&self, name: &str) -> Option<String>;
    fn set_header(&mut self, name: &str, value: &str);
}

impl DeliveryExt for Delivery {
    fn get_header(self: &Delivery, name: &str) -> Option<String> {
        if let Some(headers) = self.properties.headers().as_ref() {
            let key: ShortString = name.into();
            let value = headers.inner().get(&key);
            if let Some(AMQPValue::LongString(string)) = value {
                Some(string.to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn set_header(&mut self, name: &str, value: &str) {
        let default_headers: FieldTable = Default::default();
        let current_headers = self.properties.headers().to_owned();
        let mut headers = if let Some(headers) = current_headers {
            headers
        } else {
            default_headers
        };
        headers.insert(name.into(), AMQPValue::LongString(value.into()));
        self.properties = self.properties.clone().with_headers(headers);
    }
}

async fn connect_future(
    uri: AMQPUri,
    connection_properties: ConnectionProperties,
) -> Result<Connection, lapin::Error> {
    let client = Connection::connect_uri(uri, connection_properties).await?;
    Ok(client)
}

async fn create_channel(client: Connection) -> Result<Channel, lapin::Error> {
    let channel = client.create_channel().await?;
    channel
        .confirm_select(ConfirmSelectOptions { nowait: false })
        .await?;
    Ok(channel)
}

fn headers_to_basic_properties(headers: &BTreeMap<String, String>) -> AMQPProperties {
    let mut table = FieldTable::default();
    let mut message_id = Uuid::new_v4().to_string();
    let mut expiration = "".to_string();
    headers
        .iter()
        .for_each(|(header, value)| match header.as_str() {
            "message-id" => message_id = (*value).to_string(),
            "expiration" => expiration = (*value).to_string(),
            _ => table.insert(
                (*header).to_string().into(),
                AMQPValue::LongString((*value).to_string().into()),
            ),
        });
    let mut properties = BasicProperties::default()
        .with_headers(table)
        .with_message_id(message_id.into())
        .with_delivery_mode(2);
    if !expiration.is_empty() {
        properties = properties.with_expiration(expiration.into());
    }
    properties
}

fn add_correlation_id(headers: &mut BTreeMap<String, String>) {
    if !headers.contains_key(CORRELATION_ID_HEADER_KEY) {
        let task_local_correlation_id = MESSAGE.try_with(|message| {
            if let Some(message) = message {
                return message.get_header(CORRELATION_ID_HEADER_KEY);
            }
            None
        });
        let correlation_id = match task_local_correlation_id {
            Ok(Some(id)) => id,
            _ => Uuid::new_v4().to_string(),
        };
        headers.insert(CORRELATION_ID_HEADER_KEY.to_string(), correlation_id);
    }
}

fn add_causation_id(headers: &mut BTreeMap<String, String>) {
    if !headers.contains_key(CAUSATION_ID_HEADER_KEY) {
        let task_local_message_id = MESSAGE.try_with(|message| {
            if let Some(message) = message {
                return message.properties.message_id().clone();
            }
            None
        });
        if let Ok(Some(message_id)) = task_local_message_id {
            headers.insert(
                CAUSATION_ID_HEADER_KEY.to_string(),
                message_id.as_str().to_string(),
            );
        }
    }
}

pub fn mock_delivery() -> Delivery {
    Delivery {
        delivery_tag: 0,
        exchange: "".into(),
        routing_key: "".into(),
        redelivered: false,
        properties: Default::default(),
        data: vec![],
    }
}

pub fn recreate_delivery(
    correlation_id: Option<String>,
    causation_id: Option<String>,
    message_id: Option<String>,
) -> Delivery {
    let mut properties = BasicProperties::default();
    let mut headers = FieldTable::default();
    if let Some(message_id) = message_id {
        properties = properties.with_message_id(message_id.into());
    }
    if let Some(correlation_id) = correlation_id {
        headers.insert(
            CORRELATION_ID_HEADER_KEY.into(),
            AMQPValue::LongString(correlation_id.into()),
        );
    }
    if let Some(causation_id) = causation_id {
        headers.insert(
            CAUSATION_ID_HEADER_KEY.into(),
            AMQPValue::LongString(causation_id.into()),
        );
    }
    properties = properties.with_headers(headers);

    Delivery {
        data: vec![],
        delivery_tag: 0,
        exchange: "".into(),
        redelivered: false,
        routing_key: "".into(),
        properties,
    }
}

pub trait HasRabbit {
    type Connection: RabbitExt + Send;

    fn get_rabbit(&self) -> Self::Connection;
}

#[cfg(test)]
mod tests {
    use config::{builder::DefaultState, ConfigBuilder};
    use maplit::btreemap;

    use super::*;

    #[test]
    fn valid_uri_config_test() {
        let settings = ConfigBuilder::<DefaultState>::default()
            .add_source(config::File::with_name("tests/test-config.json"))
            .build()
            .unwrap();
        let config_uri: ConfigUri = settings.get("test-valid-uri").unwrap();
        let amqp_uri = config_uri.get_lapin_uri();
        assert_eq!(amqp_uri.authority.host, "rmq-server".to_string());
        assert_eq!(amqp_uri.authority.port, 4321);
        assert_eq!(amqp_uri.authority.userinfo.username, "user".to_string());
        assert_eq!(amqp_uri.authority.userinfo.password, "pwd".to_string());
        assert_eq!(amqp_uri.vhost, "/".to_string());
        assert_eq!(amqp_uri.query.frame_max, Some(65535));
    }

    #[test]
    fn valid_detailed_config_test() {
        let settings = ConfigBuilder::<DefaultState>::default()
            .add_source(config::File::with_name("tests/test-config.json"))
            .build()
            .unwrap();
        let config_uri: ConfigUri = settings.get("test-valid-detailed").unwrap();
        let amqp_uri = config_uri.get_lapin_uri();
        assert_eq!(amqp_uri.authority.host, "localhost".to_string());
        assert_eq!(amqp_uri.authority.port, 5672);
        assert_eq!(amqp_uri.authority.userinfo.username, "guest".to_string());
        assert_eq!(amqp_uri.authority.userinfo.password, "pwd".to_string());
        assert_eq!(amqp_uri.vhost, "/develop".to_string());
        assert_eq!(amqp_uri.query.frame_max, Some(65535));
    }

    #[test]
    fn default_detailed_config_test() {
        let settings = ConfigBuilder::<DefaultState>::default()
            .add_source(config::File::with_name("tests/test-config.json"))
            .build()
            .unwrap();
        let config_uri: ConfigUri = settings.get("test-detailed-default").unwrap();
        let amqp_uri = config_uri.get_lapin_uri();
        assert_eq!(amqp_uri.authority.host, "localhost".to_string());
        assert_eq!(amqp_uri.authority.port, 5672);
        assert_eq!(amqp_uri.authority.userinfo.username, "guest".to_string());
        assert_eq!(amqp_uri.authority.userinfo.password, "guest".to_string());
        assert_eq!(amqp_uri.vhost, "/".to_string());
    }

    #[test]
    #[should_panic]
    fn uri_not_valid_test() {
        let settings = ConfigBuilder::<DefaultState>::default()
            .add_source(config::File::with_name("tests/test-config.json"))
            .build()
            .unwrap();
        let config_uri: ConfigUri = settings.get("test-not-valid").unwrap();
        let _amqp_uri = config_uri.get_lapin_uri();
    }

    #[test]
    fn test_set_headers() {
        let mut test_delivery = mock_delivery();
        test_delivery.set_header("foo", "bar");
        assert_eq!(test_delivery.get_header("foo"), Some("bar".to_string()));
    }

    #[test]
    fn test_set_message_properties() {
        let amqp_headers = headers_to_basic_properties(&btreemap! {
            "expiration".to_string() => "1000".to_string(),
            "foo".to_string() => "bar".to_string(),
        });
        assert!(amqp_headers.headers().as_ref().unwrap().contains_key("foo"));
        assert!(!amqp_headers
            .headers()
            .as_ref()
            .unwrap()
            .contains_key("expiration"),);
        assert_eq!(
            amqp_headers.expiration().as_ref().unwrap().to_string(),
            "1000".to_string()
        );
    }

    #[test]
    fn test_set_multiple_headers() {
        let mut test_delivery = mock_delivery();
        test_delivery.set_header("foo", "bar");
        test_delivery.set_header("fizz", "buzz");
        assert_eq!(test_delivery.get_header("foo"), Some("bar".to_string()));
        assert_eq!(test_delivery.get_header("fizz"), Some("buzz".to_string()));
    }
}
