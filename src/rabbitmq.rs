use deadpool_lapin::lapin::{
    options::*,
    types::FieldTable,
    BasicProperties,
    Connection,
    ConnectionProperties,
    ConnectionState,
    tcp::{AMQPUriTcpExt, NativeTlsConnector},
    uri::AMQPUri,
};
use deadpool::managed::{self, Metrics, RecycleError, RecycleResult};
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use crate::RabbitHandler;
use std::sync::Arc;

const DEFAULT_PREFETCH_COUNT: u16 = 10;

/// TLS configuration for RabbitMQ connections.
///
/// Use this to connect to RabbitMQ over `amqps://` with custom CA certificates
/// or to skip certificate verification for development environments.
#[derive(Clone, Debug)]
pub struct RabbitTlsConfig {
    /// Path to a PEM-encoded CA certificate file to trust.
    /// When set, this certificate is added to the TLS root store.
    pub ca_cert_path: Option<String>,
    /// Skip TLS certificate verification entirely.
    /// **Warning:** This is insecure and should only be used in development/testing.
    pub skip_cert_verification: bool,
}

impl RabbitTlsConfig {
    /// Creates a TLS config that trusts a specific CA certificate.
    pub fn with_ca_cert(path: impl Into<String>) -> Self {
        Self {
            ca_cert_path: Some(path.into()),
            skip_cert_verification: false,
        }
    }

    /// Creates a TLS config that skips all certificate verification.
    /// **Warning:** This is insecure and should only be used in development/testing.
    pub fn insecure() -> Self {
        Self {
            ca_cert_path: None,
            skip_cert_verification: true,
        }
    }
}

/// A pool manager that supports both plain and TLS RabbitMQ connections.
struct TlsAwareManager {
    addr: String,
    parsed_uri: AMQPUri,
    ca_cert_pem: Option<String>,
    skip_verify: bool,
    use_tls: bool,
}

impl std::fmt::Debug for TlsAwareManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAwareManager")
            .field("addr", &sanitize_url(&self.addr))
            .field("use_tls", &self.use_tls)
            .field("skip_verify", &self.skip_verify)
            .field("has_ca_cert", &self.ca_cert_pem.is_some())
            .finish()
    }
}

impl TlsAwareManager {
    fn new(
        addr: &str,
        tls_config: Option<&RabbitTlsConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let parsed_uri: AMQPUri = addr.parse()
            .map_err(|e: String| format!("Failed to parse AMQP URI: {}", e))?;

        let (ca_cert_pem, skip_verify, use_tls) = match tls_config {
            Some(tls) => {
                let pem = match &tls.ca_cert_path {
                    Some(path) => {
                        let content = std::fs::read_to_string(path)
                            .map_err(|e| format!("Failed to read CA certificate at '{}': {}", path, e))?;
                        Some(content)
                    }
                    None => None,
                };
                (pem, tls.skip_cert_verification, true)
            }
            None => (None, false, false),
        };

        Ok(Self {
            addr: addr.to_string(),
            parsed_uri,
            ca_cert_pem,
            skip_verify,
            use_tls,
        })
    }
}

impl managed::Manager for TlsAwareManager {
    type Type = Connection;
    type Error = deadpool_lapin::lapin::Error;

    async fn create(&self) -> Result<Connection, Self::Error> {
        if !self.use_tls {
            return Connection::connect(&self.addr, ConnectionProperties::default()).await;
        }

        let ca_pem = self.ca_cert_pem.clone();
        let skip = self.skip_verify;

        let connect = move |uri: &AMQPUri| {
            uri.connect().and_then(|stream| {
                let mut builder = NativeTlsConnector::builder();

                if skip {
                    builder.danger_accept_invalid_certs(true);
                    builder.danger_accept_invalid_hostnames(true);
                }

                if let Some(ref pem) = ca_pem {
                    let cert = native_tls::Certificate::from_pem(pem.as_bytes())
                        .map_err(std::io::Error::other)?;
                    builder.add_root_certificate(cert);
                }

                let connector = builder.build().map_err(std::io::Error::other)?;
                stream.into_native_tls(&connector, &uri.authority.host)
            })
        };

        Connection::connector(
            self.parsed_uri.clone(),
            Box::new(connect),
            ConnectionProperties::default(),
        )
        .await
    }

    async fn recycle(&self, conn: &mut Connection, _: &Metrics) -> RecycleResult<Self::Error> {
        match conn.status().state() {
            ConnectionState::Connected => Ok(()),
            state => Err(RecycleError::message(format!(
                "lapin connection is in state: {:?}",
                state
            ))),
        }
    }
}

type Pool = managed::Pool<TlsAwareManager>;

/// Strips credentials from a URL for safe logging (e.g. `amqp://user:pass@host` → `amqp://***@host`).
fn sanitize_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            return format!("{}://***@{}", &url[..scheme_end], &after_scheme[at_pos + 1..]);
        }
    }
    url.to_string()
}

/// Purges all messages from a RabbitMQ queue using a standalone connection.
///
/// **Warning:** This discards ALL messages in the queue irreversibly.
/// Only use this for queues where stale messages are acceptable to lose.
pub async fn purge_queue_impl(
    url: &str,
    queue: &str,
    tls_config: Option<&RabbitTlsConfig>,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let manager = TlsAwareManager::new(url, tls_config)?;
    let pool = Pool::builder(manager).build()
        .map_err(|e| format!("Failed to create pool for purge: {}", e))?;

    purge_queue_with_pool(&pool, url, queue).await
}

/// Purges all messages from a RabbitMQ queue using an existing connection pool.
async fn purge_queue_with_pool(
    pool: &Pool,
    url: &str,
    queue: &str,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let conn = pool.get().await
        .map_err(|e| format!("Failed to connect to RabbitMQ for purge at {}: {}", sanitize_url(url), e))?;

    let channel = conn
        .create_channel()
        .await
        .map_err(|e| format!("Failed to create channel for purge: {}", e))?;

    channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| format!("Failed to declare queue '{}' for purge: {}", queue, e))?;

    let purged = channel
        .queue_purge(queue, QueuePurgeOptions::default())
        .await
        .map_err(|e| format!("Failed to purge queue '{}': {}", queue, e))?;

    log::info!("Purged {} stale message(s) from queue '{}'", purged, queue);
    Ok(purged)
}

/// An asynchronous RabbitMQ client for publishing and consuming messages.
///
/// This client uses connection pooling and automatic recovery. Supports both
/// plain AMQP and TLS-secured (`amqps://`) connections.
pub struct RabbitMQClient {
    pool: Pool,
    queue_name: String,
    url: String,
    #[allow(dead_code)]
    tls_config: Option<RabbitTlsConfig>,
}

impl RabbitMQClient {
    /// Creates a new RabbitMQ client with connection pooling (no TLS).
    pub async fn new(
        url: &str,
        queue_name: &str
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::create(url, queue_name, None).await
    }

    /// Creates a new RabbitMQ client with TLS support.
    ///
    /// Use `amqps://` in your URL when connecting with TLS.
    ///
    /// # Examples
    /// ```no_run
    /// use comms::rabbitmq::{RabbitMQClient, RabbitTlsConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// // With a CA certificate
    /// let tls = RabbitTlsConfig::with_ca_cert("/path/to/ca.pem");
    /// let client = RabbitMQClient::new_with_tls("amqps://user:pass@host:5671", "queue", tls).await?;
    ///
    /// // Skip verification (dev only)
    /// let tls = RabbitTlsConfig::insecure();
    /// let client = RabbitMQClient::new_with_tls("amqps://user:pass@host:5671", "queue", tls).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_with_tls(
        url: &str,
        queue_name: &str,
        tls_config: RabbitTlsConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::create(url, queue_name, Some(tls_config)).await
    }

    async fn create(
        url: &str,
        queue_name: &str,
        tls_config: Option<RabbitTlsConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let manager = TlsAwareManager::new(url, tls_config.as_ref())?;
        let pool = Pool::builder(manager).build()
            .map_err(|e| format!("Failed to create connection pool: {}", e))?;

        let client = Self {
            pool,
            queue_name: queue_name.to_string(),
            url: url.to_string(),
            tls_config,
        };
        client.ensure_queue(queue_name).await?;

        Ok(client)
    }

    async fn ensure_queue(&self, queue_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.pool.get().await?;
        let channel = connection.create_channel().await?;
        channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        log::info!("Queue declared: {}", queue_name);
        Ok(())
    }

    /// Registers and starts a handler.
    pub async fn register_handler<H: RabbitHandler + 'static>(
        &self,
        handler: H,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handler = Arc::new(handler);
        let queue = handler.queue_name().to_string();
        if queue != self.queue_name {
            self.ensure_queue(&queue).await?;
        }

        if handler.purge_on_startup() {
            purge_queue_with_pool(&self.pool, &self.url, &queue).await?;
        }

        let client_pool = self.pool.clone();
        
        tokio::spawn(async move {
            let consumer_tag = format!("consumer-{}", queue);
            loop {
                if cancellation_token.is_cancelled() {
                    break;
                }

                let connection = match client_pool.get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        log::error!("Pool error for {}: {}. Retry 3s", queue, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                let channel = match connection.create_channel().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        log::error!("Channel error for {}: {}. Retry 3s", queue, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                if let Err(e) = channel.basic_qos(DEFAULT_PREFETCH_COUNT, BasicQosOptions::default()).await {
                    log::error!("Failed to set QoS for {}: {}. Retry 3s", queue, e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }

                let mut consumer = match channel
                    .basic_consume(
                        &queue,
                        &consumer_tag,
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("Consume error for {}: {}. Attempting to declare queue...", queue, e);
                            if let Err(decl_err) = channel.queue_declare(
                                &queue,
                                QueueDeclareOptions { durable: true, ..Default::default() },
                                FieldTable::default(),
                            ).await {
                                log::error!("Failed to declare queue {} after consume error: {}. Retry 3s", queue, decl_err);
                            } else {
                                log::info!("Successfully declared queue {} after consume error.", queue);
                            }
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                            continue;
                        }
                    };

                log::info!("Started consumer for queue '{}'", queue);

                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => return,
                        delivery_option = consumer.next() => {
                            match delivery_option {
                                Some(Ok(delivery)) => {
                                    let h = handler.clone();
                                    tokio::spawn(async move {
                                        let payload = delivery.data.clone();
                                        match h.handle(payload).await {
                                            Ok(_) => { let _ = delivery.ack(BasicAckOptions::default()).await; }
                                            Err(e) => {
                                                log::error!("Handler error: {}", e);
                                                let _ = delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await;
                                            }
                                        }
                                    });
                                }
                                Some(Err(e)) => { log::error!("Stream error: {}", e); break; }
                                None => { log::warn!("Stream ended"); break; }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Starts an asynchronous loop to consume messages from the queue.
    pub async fn start_consuming<F, Fut>(
        &self,
        consumer_tag: &str,
        message_handler: F,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let queue_name = self.queue_name.clone();
        let pool = self.pool.clone();
        let consumer_tag = consumer_tag.to_string();
        let message_handler = Arc::new(message_handler);

        tokio::spawn(async move {
            loop {
                if cancellation_token.is_cancelled() {
                    break;
                }

                let connection = match pool.get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        log::error!("Failed to get connection from pool: {}. Retrying in 3s...", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                let channel = match connection.create_channel().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        log::error!("Failed to create channel: {}. Retrying in 3s...", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };

                if let Err(e) = channel.basic_qos(DEFAULT_PREFETCH_COUNT, BasicQosOptions::default()).await {
                    log::error!("Failed to set QoS for {}: {}. Retrying in 3s...", queue_name, e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    continue;
                }

                let mut consumer = match channel
                    .basic_consume(
                        &queue_name,
                        &consumer_tag,
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("Failed to create consumer for {}: {}. Attempting to declare queue...", queue_name, e);
                            if let Err(decl_err) = channel.queue_declare(
                                &queue_name,
                                QueueDeclareOptions { durable: true, ..Default::default() },
                                FieldTable::default(),
                            ).await {
                                log::error!("Failed to declare queue {} after consume error: {}. Retry 3s", queue_name, decl_err);
                            } else {
                                log::info!("Successfully declared queue {} after consume error.", queue_name);
                            }
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                            continue;
                        }
                    };

                log::info!("Started consuming messages from queue '{}'", queue_name);

                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            log::info!("Cancellation received, shutting down RabbitMQ consumer...");
                            return;
                        }
                        delivery_option = consumer.next() => {
                            match delivery_option {
                                Some(Ok(delivery)) => {
                                    let handler = message_handler.clone();
                                    tokio::spawn(async move {
                                        let payload = delivery.data.clone();
                                        match handler(payload).await {
                                            Ok(_) => { let _ = delivery.ack(BasicAckOptions::default()).await; }
                                            Err(e) => {
                                                log::error!("Error processing delivery: {}", e);
                                                let _ = delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await;
                                            }
                                        }
                                    });
                                }
                                Some(Err(e)) => {
                                    log::error!("Error receiving message: {}. Reconnecting consumer...", e);
                                    break;
                                }
                                None => {
                                    log::warn!("Consumer stream ended unexpectedly. Reconnecting consumer...");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Publishes a serializable message to a queue with persistent delivery.
    ///
    /// Messages are published with `delivery_mode: 2` (persistent) to survive
    /// broker restarts, matching the durable queue declarations used by this client.
    pub async fn publish<T: Serialize>(
        &self,
        message: &T,
        routing_key: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(message)?;
        let routing = routing_key.unwrap_or(&self.queue_name);

        let connection = self.pool.get().await?;
        let channel = connection.create_channel().await?;

        channel
            .basic_publish(
                "",
                routing,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?;

        log::info!("Published message to queue '{}'", routing);
        Ok(())
    }
}
