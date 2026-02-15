use crate::{RabbitMQClient, RedisClient, Config};
use tokio_util::sync::CancellationToken;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestMessage {
    id: u32,
    content: String,
}

/// Runs a comprehensive suite of integration tests against the provided configuration.
/// This can be called by implementing software to verify their environment on startup.
pub async fn run_self_tests(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_redis(config).await?;
    test_rabbitmq(config).await?;
    Ok(())
}

async fn test_redis(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut redis = RedisClient::new(&config.redis_url, config.redis_database)?;
    let key = "comms_test_key";
    let value = "comms_test_value";

    redis.set(key, &value.to_string())?;
    log::debug!("Set key '{}' to value '{}'", key, value);

    // Simulate connection loss
    log::info!("Simulating Redis connection loss...");
    redis.force_disconnect();

    let retrieved: Option<String> = redis.get(key)?;
    log::debug!("Retrieved value after reconnection: {:?}", retrieved);

    if retrieved.as_deref() == Some(value) {
        Ok(())
    } else {
        Err("Redis GET did not match SET value".into())
    }
}

async fn test_rabbitmq(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = RabbitMQClient::new(&config.rabbitmq_url, "comms_test_queue").await?;
    let test_msg = TestMessage { id: 1, content: "test".to_string() };
    let ct = CancellationToken::new();

    // Test Publish
    client.publish(&test_msg, None).await?;
    log::debug!("Published test message to RabbitMQ");

    // Test Consume (short timeout)
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    
    let handler_ct = ct.clone();
    client.start_consuming("test_tag", move |data| {
        let tx = tx.clone();
        async move {
            let msg: TestMessage = serde_json::from_slice(&data)?;
            log::debug!("Consumer received message: {:?}", msg);
            let _ = tx.send(msg).await;
            Ok(())
        }
    }, handler_ct).await?;

    log::debug!("Waiting for message from RabbitMQ consumer...");
    // Wait for message with timeout
    match tokio::time::timeout(tokio::time::Duration::from_secs(5), rx.recv()).await {
        Ok(Some(received)) => {
            log::debug!("Successfully received message from channel");
            if received == test_msg {
                ct.cancel();
                Ok(())
            } else {
                Err("RabbitMQ received message mismatch".into())
            }
        }
        _ => {
            log::error!("RabbitMQ test timed out or channel closed");
            Err("RabbitMQ test timed out waiting for message".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    #[tokio::test]
    #[ignore] // Requires running Redis/RabbitMQ
    async fn test_full_integration() {
        let _ = dotenvy::dotenv();
        crate::init_logger(); // Initialize the library's logger
        let config = Config::from_env().unwrap();
        run_self_tests(&config).await.unwrap();
    }
}
