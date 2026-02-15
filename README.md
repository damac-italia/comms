# Comms Library

A high-level Rust library for managing RabbitMQ and Redis communications in microservice environments. It provides simplified, type-safe interfaces for common operations like message publishing/consuming and key-value storage, with built-in reconnection logic and automatic JSON serialization.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
comms = { git = "https://github.com/damac-italia/comms" }
```

---

## Core Concepts & Inner Workings

### 1. Observability (Logging)
The library uses `env_logger` with a customized format. Calling `init_logger()` sets up a global logger that includes:
- Timestamps (Local time)
- Log levels (INFO, DEBUG, etc.)
- Module paths (e.g., `[comms::rabbitmq]`)
- Standardized formatting for consistency across microservices.

### 2. Configuration Management
The `Config` struct is the central hub for environment-based settings. It supports loading from a `.env` file or system environment variables:
- `RABBITMQ_URL`: Connection string for RabbitMQ.
- `REDIS_URL`: Connection string for Redis.
- `QUEUE_NAME`: Default queue for the RabbitMQ client.
- `REDIS_DATABASE`: Target database index for Redis.

### 3. RabbitMQ: Resilience & Decoupling
The `RabbitMQClient` is built on top of `deadpool-lapin`, providing:
- **Connection Pooling**: Efficient management of multiple connections.
- **Automatic Recovery**: Transparent reconnection if the RabbitMQ server becomes unavailable.
- **Async Processing**: High-performance message handling using Tokio.
- **Macro-based Subscription**: The `#[subscribe_rabbit]` attribute macro simplifies the creation of consumers by automatically handling JSON deserialization and trait implementation.

### 4. Redis: Type-Safe Storage
The `RedisClient` provides a synchronous interface designed for reliability:
- **Health Checks**: Every operation (`get`, `set`, etc.) performs a connection check and triggers an automatic retry loop if the connection is lost.
- **JSON Serialization**: Automatically handles `serde`-based serialization and deserialization, allowing you to store and retrieve complex Rust structs directly.

---

## APIs & Usage

### 1. Global Logger
Initializes the standardized logging format.

```rust
use comms;

fn main() {
    comms::init_logger();
    log::info!("Service started");
}
```

### 2. Configuration
Load settings from environment variables.

```rust
use comms::Config;

let config = Config::from_env().expect("Failed to load configuration");
```

### 3. RabbitMQ Client
Manage message queues and subscriptions.

#### Defining a Subscriber (Macro)
The easiest way to handle incoming messages. The macro generates a struct that implements `RabbitHandler`.

```rust
use comms::subscribe_rabbit;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct MyData {
    id: u32,
    task: String,
}

#[subscribe_rabbit(queue = "my_worker_queue")]
async fn handle_task(data: MyData) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Processing task: {}", data.task);
    Ok(())
}
```

#### Starting the Client
```rust
use comms::RabbitMQClient;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let client = RabbitMQClient::new("amqp://localhost", "default_queue").await.unwrap();
    let token = CancellationToken::new();

    // Register the handler defined above
    client.register_handler(handle_task {}, token.clone()).await.unwrap();
    
    // Or use the low-level consumer
    client.start_consuming("tag", |payload| async move {
        Ok(())
    }, token).await.unwrap();
}
```

#### Publishing Messages
```rust
#[derive(serde::Serialize)]
struct Alert { message: String }

client.publish(&Alert { message: "Hello".into() }, Some("target_queue")).await.unwrap();
```

### 4. Redis Client
Synchronous key-value operations with automatic reconnection.

```rust
use comms::RedisClient;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct Session { user_id: String }

fn main() {
    let mut redis = RedisClient::new("redis://127.0.0.1", 0).unwrap();
    
    let session = Session { user_id: "atom_123".into() };
    
    // Set with 1 hour TTL (3600 seconds)
    redis.set_ex("user:session", &session, 3600).unwrap();
    
    // Get and automatically deserialize
    if let Some(s): Option<Session> = redis.get("user:session").unwrap() {
        println!("Active user: {}", s.user_id);
    }
}
```

---

## Self-Tests
The library includes a `run_self_tests` function that microservices can call during startup to verify their connection to RabbitMQ and Redis.

```rust
use comms::{Config, run_self_tests};

#[tokio::main]
async fn main() {
    let config = Config::from_env().unwrap();
    if let Err(e) = run_self_tests(&config).await {
        eprintln!("Environment check failed: {}", e);
        std::process::exit(1);
    }
}
```
