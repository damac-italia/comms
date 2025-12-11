use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub rabbitmq_url: String,
    pub redis_url: String,
    pub queue_name: String,
    pub max_retries: usize,
    pub redis_database: u8,
}

impl Config {
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
