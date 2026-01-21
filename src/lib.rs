//! # Comms Library
//!
//! A high-level library for managing RabbitMQ and Redis communications.
//! This crate provides simplified clients for common operations like publishing/consuming
//! messages and key-value storage with automatic reconnection and serialization.

pub mod config;
pub mod redis;
pub mod rabbitmq;

pub use config::Config;
pub use redis::RedisClient;
pub use rabbitmq::RabbitMQClient;

use std::sync::Once;
static LOGGER_INIT: Once = Once::new();

/// Initializes the global logger with a custom format including timestamps and module paths.
///
/// Use this at the start of your application to ensure consistent logging across all components.
/// It uses `std::sync::Once` internally, so calling it multiple times is safe and will only
/// perform the initialization once.
///
/// # Example
/// ```
/// comms::init_logger();
/// ```
pub fn init_logger() {
    LOGGER_INIT.call_once(|| {
        env_logger::Builder::from_default_env()
            .format(|buf, record| {
                use std::io::Write;
                writeln!(
                    buf,
                    "{} - {:5} | [{}] {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.module_path().unwrap_or("unknown"),
                    record.args()
                )
            })
            .init();
    });
}
