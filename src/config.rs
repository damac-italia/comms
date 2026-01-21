use std::env;

/// Configuration structure for the library components.
///
/// This struct holds all necessary parameters to connect to RabbitMQ and Redis,
/// as well as operational settings like retry limits.
#[derive(Debug, Clone)]
pub struct Config {
    /// The AMQP URL for RabbitMQ (e.g., "amqp://user:pass@host:5672").
    pub rabbitmq_url: String,
    /// The Redis connection URL (e.g., "redis://host:6379").
    pub redis_url: String,
    /// The name of the RabbitMQ queue to use by default.
    pub queue_name: String,
    /// Maximum number of connection retry attempts for RabbitMQ.
    pub max_retries: usize,
    /// The Redis database index (0-15).
    pub redis_database: u8,
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// It looks for:
    /// - `RABBITMQ_URL` (default: amqp://guest:guest@localhost:5672)
    /// - `REDIS_URL` (default: redis://localhost:6379)
    /// - `QUEUE_NAME` (default: default_queue)
    /// - `MAX_RETRIES` (default: 3)
    /// - `REDIS_DATABASE` (default: 0)
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Config {
            rabbitmq_url: env::var("RABBITMQ_URL")
                .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string()),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
            queue_name: env::var("QUEUE_NAME")
                .unwrap_or_else(|_| "default_queue".to_string()),
            max_retries: env::var("MAX_RETRIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            redis_database: env::var("REDIS_DATABASE")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
        })
    }

    /// Creates a new configuration instance manually.
    pub fn new(
        rabbitmq_url: String,
        redis_url: String,
        queue_name: String,
        max_retries: usize,
        redis_database: u8,
    ) -> Self {
        Config {
            rabbitmq_url,
            redis_url,
            queue_name,
            max_retries,
            redis_database,
        }
    }
}
