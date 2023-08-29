# RabbitMQ Adapter Library

## Purpose

RabbitMQ Adapter provides an interface to a RabbitMQ message broker. It facilitates development of microservices for platform v2.

## Starting RabbitMQ on Docker

```bash
docker run --rm -d -p 25672:25672/tcp -p 4369:4369/tcp -p 5671:5671/tcp -p 5672:5672/tcp -p 15672:15672 rabbitmq:3-management
```

## Usage

The library is based on `tokio` and `futures 0.3`. To interact, use the `RabbitConnection` future.

First, create it.

```rust
let rabbit = RabbitConnection::new(ConfigUri::Uri("127.0.0.1"), None, "reply");
```

Then you can spawn a task to connect and interact with it.

```rust
tokio::spawn(
    async move {
        rabbit.connect().await?;
        rabbit.create_topic_exchange("exchange").await?;
        rabbit.create_queue("queue").await?;
        rabbit.bind_queue("queue", "exchange", "#").await?;
    }
);
```

The `consume` operation is blocking and must be spawned in a dedicated tasks.

## Limitations

Currently, the library relies on the `lapin` crate. It doesn't provide error handling and cannot reopen connections. Also, it has a number of bugs

The `lapin` crate has the following shortcomings. It is not ready for production.

- A subscription (consume) cannot be canceled.
- Only rudimentary error handling.
