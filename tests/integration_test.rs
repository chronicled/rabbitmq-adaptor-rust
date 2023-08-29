use anyhow::{anyhow, Error};
use enclose::enclose;
use futures::channel::mpsc;
use futures::prelude::*;
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use rabbitmq_adaptor::{
    display::delivery::DisplayDelivery, ConfigUri, ConsumerResult, DeliveryExt, RabbitConnection,
    RabbitExt,
};
use rand::Rng;
use test_log::test;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, instrument, Instrument};
use uuid::Uuid;

const LOCALHOST: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";

fn get_random_value() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen()
}

#[test(tokio::test)]
#[instrument]
async fn rpc_test() -> Result<(), Error> {
    let random = get_random_value();
    // create topology
    let conn = Connection::connect(LOCALHOST, ConnectionProperties::default())
        .await
        .expect("Couldn't connect to RabbitMQ");
    let channel = conn.create_channel().await?;
    let default_queue = format!("rpc_reply_{random}");
    channel
        .queue_declare(
            &default_queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let config_uri = ConfigUri::Uri(LOCALHOST.to_string());
    let rabbit = RabbitConnection::new(config_uri, None, default_queue).await?;
    let exchange = format!("exchange-rpc_{random}");
    rabbit.create_topic_exchange(exchange.clone()).await?;
    let queue = format!("queue-rpc_{random}");
    rabbit.create_queue(queue.clone()).await?;
    rabbit
        .bind_queue(queue.clone(), exchange.clone(), "#".to_string())
        .await?;
    tokio::spawn(rabbit.clone().register_consumer(
        queue.clone(),
        None,
        enclose!((rabbit) move |message| {
            // send response
            info!("Got request message. Sending reply...");
            enclose!((rabbit) async move {
                rabbit
                    .rpc_response(&message, b"payload".to_vec(), vec![])
                    .await?;
                Ok(ConsumerResult::ACK)
            }
            .in_current_span()
            .boxed())
        }),
    ));
    let rpc_request_future = async move {
        info!("Sending RPC Request");
        let delivery = rabbit
            .rpc_request(
                exchange.clone(),
                "topic".to_string(),
                b"payload".to_vec(),
                vec![],
            )
            .await?;
        info!("Reply received");
        if delivery.data == b"payload" {
            return Ok(());
        }
        Err(anyhow!("Wrong payload"))
    };
    // wait for completion of test or timeout
    timeout(Duration::from_secs(15), rpc_request_future).await?
}

#[test(tokio::test)]
#[instrument]
async fn consumer_stream_test() -> Result<(), Error> {
    let number_publishing = 10;
    let random = get_random_value();
    let default_queue = format!("default_{random}");
    // create topology
    let conn = Connection::connect(LOCALHOST, ConnectionProperties::default())
        .await
        .expect("Couldn't connect to RabbitMQ");
    let channel = conn.create_channel().await?;
    channel
        .queue_declare(
            &default_queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let config_uri = ConfigUri::Uri(LOCALHOST.to_string());
    let rabbit = RabbitConnection::new(config_uri, None, default_queue.clone()).await?;
    let exchange = format!("exchange-test_{random}");
    rabbit.create_topic_exchange(exchange.clone()).await?;
    let queue = format!("queue-test_{random}");
    rabbit.create_queue(queue.clone()).await?;
    rabbit
        .bind_queue(queue.clone(), exchange.clone(), "#".to_string())
        .await?;
    tokio::spawn(enclose!((rabbit, exchange) async move {
        let futures = (0..number_publishing).map(|_|
            rabbit
                .clone()
                .publish(
                    exchange.clone(),
                    "topic-test".to_string(),
                    b"payload".to_vec(),
                    vec![],
                ));
        futures::future::try_join_all(futures).in_current_span().await.unwrap();
    }));

    let mut consumer_stream = rabbit.clone().create_consumer_stream(queue.clone(), None);

    let collect_future = async move {
        let mut results = Vec::new();
        for _ in 0..number_publishing {
            let message = consumer_stream.next().await.unwrap();
            consumer_stream.ack(message.delivery_tag).await.unwrap();
            results.push(message);
        }
        results
    };
    // let collect_future = consumer_stream.take(number_publishing).collect::<Vec<_>>();
    let results = timeout(Duration::from_secs(15), collect_future).await?;

    let payload = std::str::from_utf8(&results[0].data).unwrap();
    info!("payload: {:?}", payload);

    assert_eq!(payload, "payload");
    Ok(())
}

#[test(tokio::test)]
#[instrument]
async fn recursive_publish_test() -> Result<(), Error> {
    let number_publishing = 10;
    // create topology
    let conn = Connection::connect(LOCALHOST, ConnectionProperties::default())
        .await
        .expect("Couldn't connect to RabbitMQ");
    let channel = conn.create_channel().await?;
    let random = get_random_value();
    let queue_name = format!("recursive_reply_{random}");
    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let (mut sender, receiver) = mpsc::channel::<()>(10000);
    let config_uri = ConfigUri::Uri(LOCALHOST.to_string());
    info!("Connecting...");
    let rabbit = RabbitConnection::new(config_uri, None, queue_name.to_string()).await?;
    let exchange = format!("exchange_{random}");
    let queue = format!("queue_{random}");
    rabbit.create_topic_exchange(exchange.clone()).await?;
    rabbit.create_queue(queue.clone()).await?;
    rabbit
        .bind_queue(queue.clone(), exchange.clone(), "#".to_string())
        .await?;
    let correlation_id = Uuid::new_v4().to_string();
    rabbit
        .clone()
        .publish(
            exchange.clone(),
            "topic".to_string(),
            b"payload".to_vec(),
            vec![("correlation-id".to_string(), correlation_id.clone())],
        )
        .await?;
    tokio::spawn(rabbit.clone().register_consumer(
        queue.clone(),
        None,
        enclose!((rabbit, exchange) move |message| {
            assert_eq!(message.get_header("correlation-id").unwrap(), correlation_id);
            sender.try_send(()).unwrap();
            debug!(message = %DisplayDelivery(&message));
            // sends out messages, resulting in a recursive loop
            enclose!((rabbit, exchange) async move {
                rabbit
                    .publish(exchange, "topic".to_string(), b"payload".to_vec(), vec![])
                    .await?;
                Ok(ConsumerResult::ACK)
            }
            .boxed())
        }),
    ));
    let result = timeout(Duration::from_secs(500), async move {
        receiver.take(number_publishing).collect::<Vec<()>>().await
    })
    .await?;
    assert_eq!(result.len(), number_publishing);
    Ok(())
}
