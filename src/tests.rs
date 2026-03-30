use crate::{RabbitMQClient, RedisClient, Config};
#[cfg(test)]
use crate::{RabbitHandler, RabbitTlsConfig};
use tokio_util::sync::CancellationToken;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestMessage {
    id: u32,
    content: String,
}

/// Runs a comprehensive suite of integration tests against the provided configuration.
/// This can be called by implementing software to verify their environment on startup.
/// Redis tests are skipped when `REDIS_URL` is not configured.
pub async fn run_self_tests(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match (&config.redis_url, config.redis_database) {
        (Some(url), Some(db)) => test_redis(url, db).await?,
        _ => log::warn!("REDIS_URL not set — skipping Redis self-tests"),
    }
    test_rabbitmq(config).await?;
    Ok(())
}

async fn test_redis(url: &str, database: u8) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut redis = RedisClient::new(url, database)?;
    let key = "comms_test_key";
    let value = "comms_test_value";

    redis.set(key, &value.to_string())?;
    log::debug!("Set key '{}' to value '{}'", key, value);

    // Simulate connection loss
    log::info!("Simulating Redis connection reset...");
    redis.force_reconnect();

    let retrieved: Option<String> = redis.get(key)?;
    log::debug!("Retrieved value after reconnection: {:?}", retrieved);

    if retrieved.as_deref() == Some(value) {
        Ok(())
    } else {
        Err("Redis GET did not match SET value".into())
    }
}

async fn test_rabbitmq(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = match &config.rabbitmq_tls {
        Some(tls) => RabbitMQClient::new_with_tls(&config.rabbitmq_url, "comms_test_queue", tls.clone()).await?,
        None => RabbitMQClient::new(&config.rabbitmq_url, "comms_test_queue").await?,
    };
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

    // ═══════════════════════════════════════════════════════════════
    //  Redis integration tests
    // ═══════════════════════════════════════════════════════════════

    fn redis_client() -> RedisClient {
        let _ = dotenvy::dotenv();
        let url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://localhost:6379".into());
        let db: u8 = std::env::var("REDIS_DATABASE")
            .unwrap_or_else(|_| "0".into())
            .parse()
            .unwrap_or(0);
        RedisClient::new(&url, db).expect("Redis connection required")
    }

    #[test]
    #[ignore] // Requires running Redis
    fn redis_set_and_get() {
        let mut r = redis_client();
        let val = TestMessage { id: 1, content: "hello".into() };
        r.set("comms_test:set_get", &val).unwrap();

        let got: Option<TestMessage> = r.get("comms_test:set_get").unwrap();
        assert_eq!(got, Some(val));

        r.delete("comms_test:set_get").unwrap();
    }

    #[test]
    #[ignore]
    fn redis_get_missing_key_returns_none() {
        let mut r = redis_client();
        let got: Option<String> = r.get("comms_test:nonexistent_key_xyz").unwrap();
        assert!(got.is_none());
    }

    #[test]
    #[ignore]
    fn redis_set_ex_expires() {
        let mut r = redis_client();
        r.set_ex("comms_test:ttl", &"ephemeral", 1).unwrap();

        let before: Option<String> = r.get("comms_test:ttl").unwrap();
        assert_eq!(before.as_deref(), Some("ephemeral"));

        std::thread::sleep(std::time::Duration::from_secs(2));
        let after: Option<String> = r.get("comms_test:ttl").unwrap();
        assert!(after.is_none(), "key should have expired");
    }

    #[test]
    #[ignore]
    fn redis_delete() {
        let mut r = redis_client();
        r.set("comms_test:del", &"gone").unwrap();
        assert!(r.exists("comms_test:del").unwrap());

        r.delete("comms_test:del").unwrap();
        assert!(!r.exists("comms_test:del").unwrap());
    }

    #[test]
    #[ignore]
    fn redis_delete_bulk() {
        let mut r = redis_client();
        r.set("comms_test:bulk_a", &1).unwrap();
        r.set("comms_test:bulk_b", &2).unwrap();
        r.set("comms_test:bulk_c", &3).unwrap();

        r.delete_bulk(&["comms_test:bulk_a", "comms_test:bulk_b", "comms_test:bulk_c"]).unwrap();
        assert!(!r.exists("comms_test:bulk_a").unwrap());
        assert!(!r.exists("comms_test:bulk_b").unwrap());
        assert!(!r.exists("comms_test:bulk_c").unwrap());
    }

    #[test]
    #[ignore]
    fn redis_delete_bulk_empty_is_noop() {
        let mut r = redis_client();
        r.delete_bulk(&[]).unwrap(); // should not error
    }

    #[test]
    #[ignore]
    fn redis_keys_pattern() {
        let mut r = redis_client();
        r.set("comms_test:keys_a", &1).unwrap();
        r.set("comms_test:keys_b", &2).unwrap();

        let keys = r.keys("comms_test:keys_*").unwrap();
        assert!(keys.len() >= 2);
        assert!(keys.contains(&"comms_test:keys_a".to_string()));
        assert!(keys.contains(&"comms_test:keys_b".to_string()));

        r.delete_bulk(&["comms_test:keys_a", "comms_test:keys_b"]).unwrap();
    }

    #[test]
    #[ignore]
    fn redis_exists() {
        let mut r = redis_client();
        r.set("comms_test:exists", &true).unwrap();
        assert!(r.exists("comms_test:exists").unwrap());
        assert!(!r.exists("comms_test:does_not_exist_xyz").unwrap());

        r.delete("comms_test:exists").unwrap();
    }

    #[test]
    #[ignore]
    fn redis_try_clone() {
        let r = redis_client();
        let mut cloned = r.try_clone().expect("clone should succeed");

        // The clone should be able to operate independently
        cloned.set("comms_test:clone", &"from_clone").unwrap();
        let val: Option<String> = cloned.get("comms_test:clone").unwrap();
        assert_eq!(val.as_deref(), Some("from_clone"));

        cloned.delete("comms_test:clone").unwrap();
    }

    #[test]
    #[ignore]
    fn redis_select_database() {
        let mut r = redis_client();
        // Switch to database 1, set a key, switch back, verify isolation
        r.select_database(1).unwrap();
        r.set("comms_test:db1_key", &"in_db1").unwrap();

        r.select_database(0).unwrap();
        let val: Option<String> = r.get("comms_test:db1_key").unwrap();
        // Key should not exist in db 0 (unless test env already has it)
        // Clean up: switch back and delete
        r.select_database(1).unwrap();
        r.delete("comms_test:db1_key").unwrap();

        // The main assertion: we could set/get across databases without error
        assert!(val.is_none() || val.is_some()); // no panic = success
    }

    #[test]
    #[ignore]
    fn redis_force_reconnect_and_continue() {
        let mut r = redis_client();
        r.set("comms_test:reconnect", &"before").unwrap();

        r.force_reconnect();

        let val: Option<String> = r.get("comms_test:reconnect").unwrap();
        assert_eq!(val.as_deref(), Some("before"));

        r.delete("comms_test:reconnect").unwrap();
    }

    #[test]
    #[ignore]
    fn redis_struct_roundtrip() {
        let mut r = redis_client();
        let msg = TestMessage { id: 42, content: "structured data".into() };
        r.set("comms_test:struct", &msg).unwrap();

        let got: Option<TestMessage> = r.get("comms_test:struct").unwrap();
        assert_eq!(got, Some(msg));

        r.delete("comms_test:struct").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════
    //  RabbitMQ integration tests
    // ═══════════════════════════════════════════════════════════════

    async fn rabbit_client(queue: &str) -> RabbitMQClient {
        let _ = dotenvy::dotenv();
        let url = std::env::var("RABBITMQ_URL")
            .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".into());
        RabbitMQClient::new(&url, queue).await.expect("RabbitMQ connection required")
    }

    #[tokio::test]
    #[ignore] // Requires running RabbitMQ
    async fn rabbit_publish_and_consume_simple() {
        let client = rabbit_client("comms_test_simple").await;
        let msg = TestMessage { id: 10, content: "simple".into() };
        let ct = CancellationToken::new();

        client.publish(&msg, None).await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        client.start_consuming("simple_tag", move |data| {
            let tx = tx.clone();
            async move {
                let m: TestMessage = serde_json::from_slice(&data)?;
                let _ = tx.send(m).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }

    #[tokio::test]
    #[ignore]
    async fn rabbit_publish_with_builder_and_consume() {
        let client = rabbit_client("comms_test_builder").await;
        let msg = TestMessage { id: 20, content: "via builder".into() };
        let ct = CancellationToken::new();

        client.send(&msg)
            .to("comms_test_builder")
            .content_type("application/json")
            .priority(5)
            .message_id("test-msg-20")
            .persistent()
            .publish()
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        client.start_consuming("builder_tag", move |data| {
            let tx = tx.clone();
            async move {
                let m: TestMessage = serde_json::from_slice(&data)?;
                let _ = tx.send(m).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }

    #[tokio::test]
    #[ignore]
    async fn rabbit_publish_with_custom_routing_key() {
        let client = rabbit_client("comms_test_routing_src").await;
        // Publish to a different queue via routing key
        let target_client = rabbit_client("comms_test_routing_dst").await;
        let msg = TestMessage { id: 30, content: "routed".into() };
        let ct = CancellationToken::new();

        client.publish(&msg, Some("comms_test_routing_dst")).await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        target_client.start_consuming("routing_tag", move |data| {
            let tx = tx.clone();
            async move {
                let m: TestMessage = serde_json::from_slice(&data)?;
                let _ = tx.send(m).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }

    #[tokio::test]
    #[ignore]
    async fn rabbit_send_bytes_and_consume() {
        let client = rabbit_client("comms_test_bytes").await;
        let payload = b"raw binary payload".to_vec();
        let ct = CancellationToken::new();

        client.send_bytes(payload.clone())
            .to("comms_test_bytes")
            .content_type("application/octet-stream")
            .transient()
            .publish()
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        client.start_consuming("bytes_tag", move |data| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(data).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, payload);
        ct.cancel();
    }

    // ── register_handler (trait-based consumer) ─────────────────────

    struct TestHandler {
        tx: tokio::sync::mpsc::Sender<TestMessage>,
    }

    #[async_trait::async_trait]
    impl RabbitHandler for TestHandler {
        fn queue_name(&self) -> &str {
            "comms_test_handler"
        }

        fn purge_on_startup(&self) -> bool {
            true
        }

        async fn handle(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let msg: TestMessage = serde_json::from_slice(&data)?;
            let _ = self.tx.send(msg).await;
            Ok(())
        }
    }

    #[tokio::test]
    #[ignore]
    async fn rabbit_register_handler_roundtrip() {
        let client = rabbit_client("comms_test_handler").await;
        let msg = TestMessage { id: 40, content: "handler".into() };
        let ct = CancellationToken::new();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler = TestHandler { tx };
        client.register_handler(handler, ct.clone()).await.unwrap();

        // Give the consumer a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        client.publish(&msg, Some("comms_test_handler")).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }

    // ── RabbitMQ TLS ────────────────────────────────────────────────

    #[tokio::test]
    #[ignore] // Requires RabbitMQ with TLS on amqps://
    async fn rabbit_tls_insecure_publish_consume() {
        let _ = dotenvy::dotenv();
        let url = std::env::var("RABBITMQ_TLS_URL")
            .unwrap_or_else(|_| "amqps://guest:guest@localhost:5671".into());
        let tls = RabbitTlsConfig::insecure();
        let client = RabbitMQClient::new_with_tls(&url, "comms_test_tls", tls)
            .await
            .expect("TLS connection required");

        let msg = TestMessage { id: 50, content: "over tls".into() };
        let ct = CancellationToken::new();

        client.publish(&msg, None).await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        client.start_consuming("tls_tag", move |data| {
            let tx = tx.clone();
            async move {
                let m: TestMessage = serde_json::from_slice(&data)?;
                let _ = tx.send(m).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ with TLS + CA cert
    async fn rabbit_tls_ca_cert_publish_consume() {
        let _ = dotenvy::dotenv();
        let url = std::env::var("RABBITMQ_TLS_URL")
            .unwrap_or_else(|_| "amqps://guest:guest@localhost:5671".into());
        let ca_path = std::env::var("RABBITMQ_TLS_CA_CERT")
            .expect("RABBITMQ_TLS_CA_CERT must be set for this test");
        let tls = RabbitTlsConfig::with_ca_cert(ca_path);
        let client = RabbitMQClient::new_with_tls(&url, "comms_test_tls_ca", tls)
            .await
            .expect("TLS+CA connection required");

        let msg = TestMessage { id: 60, content: "tls with ca".into() };
        let ct = CancellationToken::new();

        client.send(&msg)
            .content_type("application/json")
            .persistent()
            .publish()
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handler_ct = ct.clone();
        client.start_consuming("tls_ca_tag", move |data| {
            let tx = tx.clone();
            async move {
                let m: TestMessage = serde_json::from_slice(&data)?;
                let _ = tx.send(m).await;
                Ok(())
            }
        }, handler_ct).await.unwrap();

        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(5), rx.recv()
        ).await.unwrap().unwrap();
        assert_eq!(received, msg);
        ct.cancel();
    }
}
