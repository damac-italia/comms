use deadpool_lapin::lapin::{
    options::*,
    types::FieldTable,
    BasicProperties,
    Connection,
    ConnectionProperties,
    ConnectionState,
    tcp::{NativeTlsConnector, TcpStream},
    uri::AMQPUri,
};
use deadpool::managed::{self, Metrics, RecycleError, RecycleResult};
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use crate::RabbitHandler;
use std::sync::Arc;

const DEFAULT_PREFETCH_COUNT: u16 = 10;

fn tokio_conn_properties() -> ConnectionProperties {
    ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio::current())
}

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
            return Connection::connect(&self.addr, tokio_conn_properties()).await;
        }

        let ca_pem = self.ca_cert_pem.clone();
        let skip = self.skip_verify;

        let connect = move |uri: &AMQPUri| {
            // Raw TCP connect — no automatic TLS so we control the handshake.
            let stream = TcpStream::connect((uri.authority.host.as_str(), uri.authority.port))?;

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
            let stream = stream.into_native_tls(&connector, &uri.authority.host)?;
            stream.set_nonblocking(true)?;
            Ok(stream)
        };

        Connection::connector(
            self.parsed_uri.clone(),
            Box::new(connect),
            tokio_conn_properties(),
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

    /// Creates a fluent message builder for publishing.
    ///
    /// # Examples
    /// ```no_run
    /// # async fn example(client: &comms::RabbitMQClient) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Order { id: u32, item: String }
    ///
    /// // Simple — send to the client's default queue
    /// client.send(&Order { id: 1, item: "widget".into() }).publish().await?;
    ///
    /// // Fluent — target a specific queue with metadata
    /// client.send(&Order { id: 2, item: "gadget".into() })
    ///     .to("orders.priority")
    ///     .priority(9)
    ///     .expires_in_secs(60)
    ///     .message_id("ord-002")
    ///     .publish()
    ///     .await?;
    ///
    /// // Raw bytes
    /// client.send_bytes(b"ping".to_vec())
    ///     .to("health")
    ///     .content_type("text/plain")
    ///     .transient()
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send<'a, T: Serialize>(&'a self, message: &T) -> MessageBuilder<'a> {
        let payload = serde_json::to_vec(message).expect("failed to serialize message");
        MessageBuilder::new(self, payload)
    }

    /// Creates a fluent message builder from raw bytes.
    pub fn send_bytes(&self, payload: Vec<u8>) -> MessageBuilder<'_> {
        MessageBuilder::new(self, payload)
    }

    async fn publish_raw(
        &self,
        payload: &[u8],
        routing_key: &str,
        exchange: &str,
        properties: BasicProperties,
        options: BasicPublishOptions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.pool.get().await?;
        let channel = connection.create_channel().await?;

        channel
            .basic_publish(exchange, routing_key, options, payload, properties)
            .await?;

        log::info!("Published message to '{}'", routing_key);
        Ok(())
    }
}

/// A fluent builder for constructing and publishing RabbitMQ messages.
///
/// Created via [`RabbitMQClient::send`] or [`RabbitMQClient::send_bytes`].
/// Call [`.publish().await`](MessageBuilder::publish) to send the message.
pub struct MessageBuilder<'a> {
    client: &'a RabbitMQClient,
    payload: Vec<u8>,
    routing_key: Option<String>,
    exchange: String,
    properties: BasicProperties,
    options: BasicPublishOptions,
}

impl<'a> MessageBuilder<'a> {
    fn new(client: &'a RabbitMQClient, payload: Vec<u8>) -> Self {
        Self {
            client,
            payload,
            routing_key: None,
            exchange: String::new(),
            properties: BasicProperties::default().with_delivery_mode(2),
            options: BasicPublishOptions::default(),
        }
    }

    /// Sets the target queue / routing key. Defaults to the client's queue.
    pub fn to(mut self, queue: &str) -> Self {
        self.routing_key = Some(queue.to_string());
        self
    }

    /// Sets the exchange to publish to. Defaults to the default exchange ("").
    pub fn exchange(mut self, exchange: &str) -> Self {
        self.exchange = exchange.to_string();
        self
    }

    /// Sets the message content type (e.g. "application/json").
    pub fn content_type(mut self, ct: &str) -> Self {
        self.properties = self.properties.with_content_type(ct.into());
        self
    }

    /// Sets the message content encoding (e.g. "gzip").
    pub fn content_encoding(mut self, encoding: &str) -> Self {
        self.properties = self.properties.with_content_encoding(encoding.into());
        self
    }

    /// Marks the message as persistent (delivery_mode = 2). This is the default.
    pub fn persistent(mut self) -> Self {
        self.properties = self.properties.with_delivery_mode(2);
        self
    }

    /// Marks the message as transient (delivery_mode = 1). It will not survive broker restarts.
    pub fn transient(mut self) -> Self {
        self.properties = self.properties.with_delivery_mode(1);
        self
    }

    /// Sets the message priority (0–9, where 9 is highest).
    pub fn priority(mut self, priority: u8) -> Self {
        self.properties = self.properties.with_priority(priority);
        self
    }

    /// Sets the correlation ID, typically used for RPC-style request/response patterns.
    pub fn correlation_id(mut self, id: &str) -> Self {
        self.properties = self.properties.with_correlation_id(id.into());
        self
    }

    /// Sets the reply-to queue for RPC-style patterns.
    pub fn reply_to(mut self, queue: &str) -> Self {
        self.properties = self.properties.with_reply_to(queue.into());
        self
    }

    /// Sets a TTL (time-to-live) on the message as a raw string (milliseconds).
    pub fn expiration(mut self, ms: &str) -> Self {
        self.properties = self.properties.with_expiration(ms.into());
        self
    }

    /// Convenience: sets the message TTL in seconds.
    pub fn expires_in_secs(self, secs: u64) -> Self {
        self.expiration(&(secs * 1000).to_string())
    }

    /// Sets a unique message ID.
    pub fn message_id(mut self, id: &str) -> Self {
        self.properties = self.properties.with_message_id(id.into());
        self
    }

    /// Sets the message timestamp (unix epoch seconds).
    pub fn timestamp(mut self, ts: u64) -> Self {
        self.properties = self.properties.with_timestamp(ts);
        self
    }

    /// Sets the message type identifier.
    pub fn message_type(mut self, t: &str) -> Self {
        self.properties = self.properties.with_type(t.into());
        self
    }

    /// Sets the application ID.
    pub fn app_id(mut self, id: &str) -> Self {
        self.properties = self.properties.with_app_id(id.into());
        self
    }

    /// Sets custom headers on the message.
    pub fn headers(mut self, headers: FieldTable) -> Self {
        self.properties = self.properties.with_headers(headers);
        self
    }

    /// Enables the `mandatory` flag — the broker will return the message if it
    /// can't be routed to any queue.
    pub fn mandatory(mut self) -> Self {
        self.options.mandatory = true;
        self
    }

    /// Enables the `immediate` flag.
    pub fn immediate(mut self) -> Self {
        self.options.immediate = true;
        self
    }

    /// Publishes the message to the broker.
    pub async fn publish(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let routing = self.routing_key.as_deref()
            .unwrap_or(&self.client.queue_name);

        self.client
            .publish_raw(&self.payload, routing, &self.exchange, self.properties, self.options)
            .await
    }

    #[cfg(test)]
    fn routing_key(&self) -> Option<&str> {
        self.routing_key.as_deref()
    }

    #[cfg(test)]
    fn exchange_name(&self) -> &str {
        &self.exchange
    }

    #[cfg(test)]
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[cfg(test)]
    fn properties(&self) -> &BasicProperties {
        &self.properties
    }

    #[cfg(test)]
    fn options(&self) -> &BasicPublishOptions {
        &self.options
    }
}

#[cfg(test)]
impl RabbitMQClient {
    /// Creates a lightweight client for unit tests. The pool is valid but
    /// never contacted — tests must not call methods that hit the broker.
    fn test_instance(queue_name: &str) -> Self {
        let mgr = TlsAwareManager::new("amqp://guest:guest@localhost/%2f", None)
            .expect("test URI should parse");
        let pool = Pool::builder(mgr).max_size(1).build()
            .expect("pool build should succeed");
        Self {
            pool,
            queue_name: queue_name.to_string(),
            url: "amqp://localhost".to_string(),
            tls_config: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Order {
        id: u32,
        item: String,
    }

    fn client() -> RabbitMQClient {
        RabbitMQClient::test_instance("default_queue")
    }

    // ── Builder defaults ────────────────────────────────────────────

    #[test]
    fn builder_defaults_are_persistent_with_empty_exchange() {
        let c = client();
        let b = c.send_bytes(b"hello".to_vec());

        assert_eq!(b.payload(), b"hello");
        assert_eq!(b.routing_key(), None);
        assert_eq!(b.exchange_name(), "");
        assert_eq!(b.properties().delivery_mode(), &Some(2));
        assert!(!b.options().mandatory);
        assert!(!b.options().immediate);
    }

    // ── Serialization via send() ────────────────────────────────────

    #[test]
    fn send_serializes_message_to_json() {
        let c = client();
        let order = Order { id: 42, item: "widget".into() };
        let b = c.send(&order);

        let parsed: serde_json::Value = serde_json::from_slice(b.payload()).unwrap();
        assert_eq!(parsed["id"], 42);
        assert_eq!(parsed["item"], "widget");
    }

    // ── Routing / exchange ──────────────────────────────────────────

    #[test]
    fn to_sets_routing_key() {
        let c = client();
        let b = c.send_bytes(vec![]).to("orders.priority");
        assert_eq!(b.routing_key(), Some("orders.priority"));
    }

    #[test]
    fn exchange_sets_exchange() {
        let c = client();
        let b = c.send_bytes(vec![]).exchange("my_exchange");
        assert_eq!(b.exchange_name(), "my_exchange");
    }

    // ── Property setters ────────────────────────────────────────────

    #[test]
    fn content_type_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).content_type("application/json");
        assert_eq!(
            b.properties().content_type().as_ref().map(|s| s.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn content_encoding_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).content_encoding("gzip");
        assert_eq!(
            b.properties().content_encoding().as_ref().map(|s| s.as_str()),
            Some("gzip")
        );
    }

    #[test]
    fn transient_sets_delivery_mode_1() {
        let c = client();
        let b = c.send_bytes(vec![]).transient();
        assert_eq!(b.properties().delivery_mode(), &Some(1));
    }

    #[test]
    fn persistent_sets_delivery_mode_2() {
        let c = client();
        let b = c.send_bytes(vec![]).transient().persistent();
        assert_eq!(b.properties().delivery_mode(), &Some(2));
    }

    #[test]
    fn priority_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).priority(9);
        assert_eq!(b.properties().priority(), &Some(9));
    }

    #[test]
    fn correlation_id_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).correlation_id("req-123");
        assert_eq!(
            b.properties().correlation_id().as_ref().map(|s| s.as_str()),
            Some("req-123")
        );
    }

    #[test]
    fn reply_to_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).reply_to("reply_queue");
        assert_eq!(
            b.properties().reply_to().as_ref().map(|s| s.as_str()),
            Some("reply_queue")
        );
    }

    #[test]
    fn expiration_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).expiration("30000");
        assert_eq!(
            b.properties().expiration().as_ref().map(|s| s.as_str()),
            Some("30000")
        );
    }

    #[test]
    fn expires_in_secs_converts_to_millis() {
        let c = client();
        let b = c.send_bytes(vec![]).expires_in_secs(60);
        assert_eq!(
            b.properties().expiration().as_ref().map(|s| s.as_str()),
            Some("60000")
        );
    }

    #[test]
    fn message_id_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).message_id("msg-456");
        assert_eq!(
            b.properties().message_id().as_ref().map(|s| s.as_str()),
            Some("msg-456")
        );
    }

    #[test]
    fn timestamp_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).timestamp(1_700_000_000);
        assert_eq!(b.properties().timestamp(), &Some(1_700_000_000));
    }

    #[test]
    fn message_type_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).message_type("order.created");
        assert_eq!(
            b.properties().kind().as_ref().map(|s| s.as_str()),
            Some("order.created")
        );
    }

    #[test]
    fn app_id_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).app_id("my-service");
        assert_eq!(
            b.properties().app_id().as_ref().map(|s| s.as_str()),
            Some("my-service")
        );
    }

    #[test]
    fn headers_are_set() {
        let c = client();
        let mut headers = FieldTable::default();
        headers.insert("x-retry".into(), deadpool_lapin::lapin::types::AMQPValue::LongInt(3));
        let b = c.send_bytes(vec![]).headers(headers);
        assert!(b.properties().headers().is_some());
    }

    // ── Publish options ─────────────────────────────────────────────

    #[test]
    fn mandatory_flag_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).mandatory();
        assert!(b.options().mandatory);
    }

    #[test]
    fn immediate_flag_is_set() {
        let c = client();
        let b = c.send_bytes(vec![]).immediate();
        assert!(b.options().immediate);
    }

    // ── Chaining ────────────────────────────────────────────────────

    #[test]
    fn full_chain_builds_correctly() {
        let c = client();
        let order = Order { id: 1, item: "gadget".into() };
        let b = c.send(&order)
            .to("orders.priority")
            .exchange("orders_exchange")
            .priority(9)
            .expires_in_secs(30)
            .message_id("ord-001")
            .correlation_id("corr-99")
            .reply_to("responses")
            .content_type("application/json")
            .app_id("order-service")
            .mandatory();

        assert_eq!(b.routing_key(), Some("orders.priority"));
        assert_eq!(b.exchange_name(), "orders_exchange");
        assert_eq!(b.properties().priority(), &Some(9));
        assert_eq!(
            b.properties().expiration().as_ref().map(|s| s.as_str()),
            Some("30000")
        );
        assert_eq!(
            b.properties().message_id().as_ref().map(|s| s.as_str()),
            Some("ord-001")
        );
        assert!(b.options().mandatory);
        assert!(!b.options().immediate);
    }

    #[test]
    fn send_bytes_preserves_raw_payload() {
        let c = client();
        let raw = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let b = c.send_bytes(raw.clone()).to("binary_queue").transient();

        assert_eq!(b.payload(), &raw);
        assert_eq!(b.properties().delivery_mode(), &Some(1));
    }

    // ── RabbitTlsConfig ─────────────────────────────────────────────

    #[test]
    fn tls_config_with_ca_cert() {
        let cfg = RabbitTlsConfig::with_ca_cert("/path/to/ca.pem");
        assert_eq!(cfg.ca_cert_path.as_deref(), Some("/path/to/ca.pem"));
        assert!(!cfg.skip_cert_verification);
    }

    #[test]
    fn tls_config_insecure() {
        let cfg = RabbitTlsConfig::insecure();
        assert!(cfg.ca_cert_path.is_none());
        assert!(cfg.skip_cert_verification);
    }

    #[test]
    fn tls_config_with_ca_cert_accepts_string() {
        let path = String::from("/etc/ssl/certs/ca.pem");
        let cfg = RabbitTlsConfig::with_ca_cert(path);
        assert_eq!(cfg.ca_cert_path.as_deref(), Some("/etc/ssl/certs/ca.pem"));
    }

    #[test]
    fn tls_config_is_cloneable() {
        let cfg = RabbitTlsConfig::with_ca_cert("/ca.pem");
        let cloned = cfg.clone();
        assert_eq!(cloned.ca_cert_path, cfg.ca_cert_path);
        assert_eq!(cloned.skip_cert_verification, cfg.skip_cert_verification);
    }

    // ── TlsAwareManager construction ────────────────────────────────

    #[test]
    fn manager_new_plain_connection() {
        let mgr = TlsAwareManager::new("amqp://guest:guest@localhost:5672/%2f", None);
        assert!(mgr.is_ok());
        let mgr = mgr.unwrap();
        assert!(!mgr.use_tls);
        assert!(!mgr.skip_verify);
        assert!(mgr.ca_cert_pem.is_none());
    }

    #[test]
    fn manager_new_with_insecure_tls() {
        let tls = RabbitTlsConfig::insecure();
        let mgr = TlsAwareManager::new("amqps://user:pass@host:5671/%2f", Some(&tls));
        assert!(mgr.is_ok());
        let mgr = mgr.unwrap();
        assert!(mgr.use_tls);
        assert!(mgr.skip_verify);
        assert!(mgr.ca_cert_pem.is_none());
    }

    #[test]
    fn manager_new_rejects_invalid_uri() {
        let result = TlsAwareManager::new("not-a-valid-uri", None);
        assert!(result.is_err());
    }

    // ── sanitize_url ────────────────────────────────────────────────

    #[test]
    fn sanitize_url_strips_credentials() {
        assert_eq!(
            sanitize_url("amqp://user:pass@host:5672"),
            "amqp://***@host:5672"
        );
    }

    #[test]
    fn sanitize_url_no_credentials() {
        assert_eq!(
            sanitize_url("amqp://localhost:5672"),
            "amqp://localhost:5672"
        );
    }

    #[test]
    fn sanitize_url_preserves_vhost() {
        assert_eq!(
            sanitize_url("amqp://user:pass@host:5672/%2f"),
            "amqp://***@host:5672/%2f"
        );
    }
}
