# comms

![license](https://img.shields.io/badge/license-MIT-blue) ![language](https://img.shields.io/badge/rust-2024-orange)

Library for RabbitMQ and Redis communication.

High-level Rust clients for microservice messaging: pooled + auto-reconnecting RabbitMQ publishing/consuming and a synchronous JSON-serializing Redis client. TLS support for RabbitMQ, an attribute macro for defining consumers, and env-based configuration.

## Use case

Microservices that need a thin, consistent layer over `deadpool-lapin` (RabbitMQ) and `redis`, with connection resilience, JSON (de)serialization, standardized logging, and startup self-tests baked in — so each service does not re-implement pooling, retry loops, and consumer boilerplate.

## Features

Grounded in the public API (`src/lib.rs`):

- **RabbitMQ client** with connection pooling and automatic recovery — `RabbitMQClient` (`src/rabbitmq.rs:234`), built on `deadpool-lapin`.
- **TLS / `amqps://` support** — `RabbitTlsConfig::with_ca_cert` / `RabbitTlsConfig::insecure`, `RabbitMQClient::new_with_tls` (`src/rabbitmq.rs:40`, `:270`).
- **Consumer macro** — `#[subscribe_rabbit(queue = "...")]` generates a `RabbitHandler` impl with JSON deserialization; optional `purge_on_startup = true` (`comms-macros/src/lib.rs:6`).
- **Two consume paths** — `register_handler` (uses a `RabbitHandler`) and `start_consuming` (raw closure over `Vec<u8>`); both auto-ack on success, nack+requeue on error, and reconnect on failure (`src/rabbitmq.rs:316`, `:422`).
- **Fluent publish builder** — `client.send(&msg)` / `client.send_bytes(bytes)` → `MessageBuilder` with `.to`, `.exchange`, `.priority`, `.expires_in_secs`, `.correlation_id`, `.reply_to`, `.headers`, `.persistent`/`.transient`, `.mandatory`, etc. (`src/rabbitmq.rs:628`). Simple path: `publish(&msg, routing_key)` (`src/rabbitmq.rs:537`).
- **Queue purge helpers** — `purge_queue` / `purge_queue_tls` (`src/lib.rs:23`, `:35`). **Irreversibly discards all messages in the queue.**
- **Synchronous Redis client** with health-checked auto-reconnect and JSON (de)serialization — `RedisClient` (`src/redis.rs:36`): `get`, `set`, `set_ex`, `delete`, `delete_bulk`, `keys`, `exists`, `select_database`, `try_clone`.
- **Standardized logger** — `init_logger()` sets an `env_logger` format with local timestamps, level, and module path; safe to call multiple times (`src/lib.rs:67`).
- **Startup self-tests** — `run_self_tests(&config)` verifies RabbitMQ (and Redis, when configured) connectivity (`src/tests.rs:16`).
- **Env-based config** — `Config::from_env` (`src/config.rs:33`).

## Requirements

- **Rust with edition 2024** (`Cargo.toml`) — requires Rust 1.85 or newer.
- A reachable **RabbitMQ** broker; optionally **Redis**.
- System TLS libraries for `native-tls` (used by `deadpool-lapin` + `native-tls`).

## Installation

Not published to crates.io — add via git (`repository` in `Cargo.toml`):

```toml
[dependencies]
comms = { git = "https://github.com/damac-italia/comms" }
```

## Usage

### Logger

```rust
fn main() {
    comms::init_logger();
    log::info!("Service started");
}
```

### Configuration from environment

```rust
use comms::Config;

let config = Config::from_env().expect("failed to load configuration");
```

### Define a consumer with the macro

`#[subscribe_rabbit]` generates a struct (named after the function) that implements `RabbitHandler`, deserializing the payload into the argument type via `serde_json` (`comms-macros/src/lib.rs`).

```rust
use comms::subscribe_rabbit;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct MyData {
    id: u32,
    task: String,
}

// `queue` is required; `purge_on_startup` is optional (default false).
#[subscribe_rabbit(queue = "my_worker_queue", purge_on_startup = false)]
async fn handle_task(data: MyData) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("processing: {}", data.task);
    Ok(())
}
```

### Start the client and register the handler

```rust
use comms::RabbitMQClient;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let client = RabbitMQClient::new("amqp://guest:guest@localhost:5672", "default_queue")
        .await
        .unwrap();
    let token = CancellationToken::new();

    // Handler-based (from the macro above)
    client.register_handler(handle_task {}, token.clone()).await.unwrap();

    // Or a low-level closure consumer on the client's default queue
    client.start_consuming("my-tag", |payload: Vec<u8>| async move {
        // process raw bytes
        Ok(())
    }, token).await.unwrap();
}
```

### Publishing

Simple publish to the default queue or a routing key:

```rust
#[derive(serde::Serialize)]
struct Alert { message: String }

client.publish(&Alert { message: "hello".into() }, Some("target_queue")).await.unwrap();
```

Fluent builder:

```rust
use serde::Serialize;

#[derive(Serialize)]
struct Order { id: u32, item: String }

// Default queue, persistent (delivery_mode = 2)
client.send(&Order { id: 1, item: "widget".into() }).publish().await.unwrap();

// Targeted with metadata
client.send(&Order { id: 2, item: "gadget".into() })
    .to("orders.priority")
    .priority(9)
    .expires_in_secs(60)
    .message_id("ord-002")
    .publish()
    .await
    .unwrap();

// Raw bytes, transient
client.send_bytes(b"ping".to_vec())
    .to("health")
    .content_type("text/plain")
    .transient()
    .publish()
    .await
    .unwrap();
```

### TLS (amqps)

```rust
use comms::{RabbitMQClient, RabbitTlsConfig};

// Trust a CA certificate
let tls = RabbitTlsConfig::with_ca_cert("/path/to/ca.pem");
let client = RabbitMQClient::new_with_tls("amqps://user:pass@host:5671", "queue", tls).await.unwrap();

// Skip verification (development only — insecure)
let tls = RabbitTlsConfig::insecure();
let client = RabbitMQClient::new_with_tls("amqps://user:pass@host:5671", "queue", tls).await.unwrap();
```

### Redis

All `RedisClient` operations are **synchronous and block the calling thread**. From async code, wrap them in `tokio::task::spawn_blocking` (`src/redis.rs:26`).

```rust
use comms::RedisClient;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct Session { user_id: String }

fn main() {
    let mut redis = RedisClient::new("redis://127.0.0.1:6379", 0).unwrap();

    let session = Session { user_id: "atom_123".into() };

    // Set with a 1 hour TTL (3600 seconds)
    redis.set_ex("user:session", &session, 3600).unwrap();

    // Get and deserialize
    let s: Option<Session> = redis.get("user:session").unwrap();
    if let Some(s) = s {
        println!("active user: {}", s.user_id);
    }
}
```

### Self-tests

```rust
use comms::{Config, run_self_tests};

#[tokio::main]
async fn main() {
    let config = Config::from_env().unwrap();
    if let Err(e) = run_self_tests(&config).await {
        eprintln!("environment check failed: {}", e);
        std::process::exit(1);
    }
}
```

## Configuration

`Config::from_env` reads (`src/config.rs:33`):

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `RABBITMQ_URL` | no | `amqp://guest:guest@localhost:5672` | AMQP/AMQPS connection string |
| `REDIS_URL` | no | *(unset → Redis disabled)* | Redis connection string; when unset, Redis features are skipped |
| `QUEUE_NAME` | no | `default_queue` | Default queue for the RabbitMQ client |
| `REDIS_DATABASE` | no | `0` | Redis database index (only used when `REDIS_URL` is set) |
| `RABBITMQ_TLS_CA_CERT` | no | — | Path to a PEM CA certificate to trust |
| `RABBITMQ_TLS_SKIP_VERIFY` | no | `false` | `true`/`1` to skip TLS certificate verification (insecure) |

`Config` can also be built directly via `Config::new` / `Config::new_with_tls` (`src/config.rs:72`, `:88`).

`.env` files: `dotenvy` is a dev-dependency used by the test suite; `from_env` itself reads process environment variables — load a `.env` in your own binary if desired.

## Project structure

```
.
├── Cargo.toml            # crate manifest (name, deps, MIT license)
├── src/
│   ├── lib.rs            # public exports, init_logger, purge_queue*, RabbitHandler
│   ├── config.rs         # Config + from_env
│   ├── rabbitmq.rs       # RabbitMQClient, MessageBuilder, TLS, purge impl
│   ├── redis.rs          # RedisClient (sync, JSON, auto-reconnect)
│   └── tests.rs          # run_self_tests + integration tests
└── comms-macros/         # proc-macro crate providing #[subscribe_rabbit]
    └── src/lib.rs
```

## License

MIT (`Cargo.toml`).

<!-- TODO: no LICENSE file is present in the repo despite `license = "MIT"` in Cargo.toml — add a LICENSE file. -->
