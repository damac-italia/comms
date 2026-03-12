use std::env;
use crate::RabbitTlsConfig;

/// Configuration structure for the library components.
///
/// This struct holds all necessary parameters to connect to RabbitMQ and Redis,
/// as well as operational settings like retry limits.
#[derive(Debug, Clone)]
pub struct Config {
    /// The AMQP URL for RabbitMQ (e.g., "amqp://user:pass@host:5672" or "amqps://user:pass@host:5671").
    pub rabbitmq_url: String,
    /// The Redis connection URL (e.g., "redis://host:6379").
    pub redis_url: String,
    /// The name of the RabbitMQ queue to use by default.
    pub queue_name: String,
    /// The Redis database index (0-15).
    pub redis_database: u8,
    /// Optional TLS configuration for RabbitMQ connections.
    pub rabbitmq_tls: Option<RabbitTlsConfig>,
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// It looks for:
    /// - `RABBITMQ_URL` (default: amqp://guest:guest@localhost:5672)
    /// - `REDIS_URL` (default: redis://localhost:6379)
    /// - `QUEUE_NAME` (default: default_queue)
    /// - `REDIS_DATABASE` (default: 0)
    /// - `RABBITMQ_TLS_CA_CERT` (optional: path to PEM CA certificate)
    /// - `RABBITMQ_TLS_SKIP_VERIFY` (optional: set to "true" to skip TLS verification)
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let ca_cert_path = env::var("RABBITMQ_TLS_CA_CERT").ok();
        let skip_verify = env::var("RABBITMQ_TLS_SKIP_VERIFY")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let rabbitmq_tls = if ca_cert_path.is_some() || skip_verify {
            Some(RabbitTlsConfig {
                ca_cert_path,
                skip_cert_verification: skip_verify,
            })
        } else {
            None
        };

        Ok(Config {
            rabbitmq_url: env::var("RABBITMQ_URL")
                .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string()),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string()),
            queue_name: env::var("QUEUE_NAME")
                .unwrap_or_else(|_| "default_queue".to_string()),
            redis_database: env::var("REDIS_DATABASE")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            rabbitmq_tls,
        })
    }

    /// Creates a new configuration instance manually.
    pub fn new(
        rabbitmq_url: String,
        redis_url: String,
        queue_name: String,
        redis_database: u8,
    ) -> Self {
        Config {
            rabbitmq_url,
            redis_url,
            queue_name,
            redis_database,
            rabbitmq_tls: None,
        }
    }

    /// Creates a new configuration instance with TLS support.
    pub fn new_with_tls(
        rabbitmq_url: String,
        redis_url: String,
        queue_name: String,
        redis_database: u8,
        tls_config: RabbitTlsConfig,
    ) -> Self {
        Config {
            rabbitmq_url,
            redis_url,
            queue_name,
            redis_database,
            rabbitmq_tls: Some(tls_config),
        }
    }
}
