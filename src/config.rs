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
    /// `None` when `REDIS_URL` is not set — Redis features will be skipped.
    pub redis_url: Option<String>,
    /// The name of the RabbitMQ queue to use by default.
    pub queue_name: String,
    /// The Redis database index (0-15). `None` when Redis is not configured.
    pub redis_database: Option<u8>,
    /// Optional TLS configuration for RabbitMQ connections.
    pub rabbitmq_tls: Option<RabbitTlsConfig>,
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// It looks for:
    /// - `RABBITMQ_URL` (default: amqp://guest:guest@localhost:5672)
    /// - `REDIS_URL` (optional — if unset, Redis features are disabled)
    /// - `QUEUE_NAME` (default: default_queue)
    /// - `REDIS_DATABASE` (default: 0, only used when `REDIS_URL` is set)
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

        let redis_url = env::var("REDIS_URL").ok();
        let redis_database = if redis_url.is_some() {
            Some(
                env::var("REDIS_DATABASE")
                    .unwrap_or_else(|_| "0".to_string())
                    .parse()
                    .unwrap_or(0),
            )
        } else {
            None
        };

        Ok(Config {
            rabbitmq_url: env::var("RABBITMQ_URL")
                .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string()),
            redis_url,
            queue_name: env::var("QUEUE_NAME")
                .unwrap_or_else(|_| "default_queue".to_string()),
            redis_database,
            rabbitmq_tls,
        })
    }

    /// Creates a new configuration instance manually.
    pub fn new(
        rabbitmq_url: String,
        redis_url: Option<String>,
        queue_name: String,
        redis_database: Option<u8>,
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
        redis_url: Option<String>,
        queue_name: String,
        redis_database: Option<u8>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_config_without_tls() {
        let cfg = Config::new(
            "amqp://localhost".into(),
            Some("redis://localhost".into()),
            "my_queue".into(),
            Some(3),
        );
        assert_eq!(cfg.rabbitmq_url, "amqp://localhost");
        assert_eq!(cfg.redis_url.as_deref(), Some("redis://localhost"));
        assert_eq!(cfg.queue_name, "my_queue");
        assert_eq!(cfg.redis_database, Some(3));
        assert!(cfg.rabbitmq_tls.is_none());
    }

    #[test]
    fn new_creates_config_without_redis() {
        let cfg = Config::new(
            "amqp://localhost".into(),
            None,
            "my_queue".into(),
            None,
        );
        assert!(cfg.redis_url.is_none());
        assert!(cfg.redis_database.is_none());
    }

    #[test]
    fn new_with_tls_stores_tls_config() {
        let tls = RabbitTlsConfig::insecure();
        let cfg = Config::new_with_tls(
            "amqps://host".into(),
            Some("redis://host".into()),
            "q".into(),
            Some(0),
            tls,
        );
        let tls = cfg.rabbitmq_tls.unwrap();
        assert!(tls.skip_cert_verification);
        assert!(tls.ca_cert_path.is_none());
    }

    #[test]
    fn from_env_defaults_and_tls_override() {
        // SAFETY: test-only; env var mutations are not thread-safe but
        // this single test exercises both code paths sequentially.
        unsafe {
            std::env::remove_var("RABBITMQ_URL");
            std::env::remove_var("REDIS_URL");
            std::env::remove_var("QUEUE_NAME");
            std::env::remove_var("REDIS_DATABASE");
            std::env::remove_var("RABBITMQ_TLS_CA_CERT");
            std::env::remove_var("RABBITMQ_TLS_SKIP_VERIFY");
        }

        // Without REDIS_URL → redis fields are None
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.rabbitmq_url, "amqp://guest:guest@localhost:5672");
        assert!(cfg.redis_url.is_none());
        assert_eq!(cfg.queue_name, "default_queue");
        assert!(cfg.redis_database.is_none());
        assert!(cfg.rabbitmq_tls.is_none());

        // With REDIS_URL set → redis fields populated
        unsafe {
            std::env::set_var("REDIS_URL", "redis://myhost:6379");
        }
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.redis_url.as_deref(), Some("redis://myhost:6379"));
        assert_eq!(cfg.redis_database, Some(0));

        // With TLS skip-verify → TLS config present
        unsafe {
            std::env::set_var("RABBITMQ_TLS_SKIP_VERIFY", "true");
        }
        let cfg = Config::from_env().unwrap();
        let tls = cfg.rabbitmq_tls.expect("TLS config should be present");
        assert!(tls.skip_cert_verification);
        assert!(tls.ca_cert_path.is_none());

        unsafe {
            std::env::remove_var("REDIS_URL");
            std::env::remove_var("RABBITMQ_TLS_SKIP_VERIFY");
        }
    }

    #[test]
    fn config_is_cloneable() {
        let cfg = Config::new("a".into(), Some("b".into()), "c".into(), Some(1));
        let cloned = cfg.clone();
        assert_eq!(cloned.rabbitmq_url, cfg.rabbitmq_url);
        assert_eq!(cloned.redis_database, cfg.redis_database);
    }
}
