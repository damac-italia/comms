use deadpool_lapin::{Config as PoolConfig, Pool, Runtime};
use deadpool_lapin::lapin::{
    options::*,
    types::FieldTable,
    BasicProperties,
};
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use crate::RabbitHandler;
use std::sync::Arc;

const DEFAULT_PREFETCH_COUNT: u16 = 10;

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
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = PoolConfig::default();
    cfg.url = Some(url.to_string());
    let pool = cfg.create_pool(Some(Runtime::Tokio1))
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
/// This client uses `deadpool-lapin` for connection pooling and automatic recovery.
pub struct RabbitMQClient {
    pool: Pool,
    queue_name: String,
    url: String,
}

impl RabbitMQClient {
    /// Creates a new RabbitMQ client with connection pooling.
    pub async fn new(
        url: &str,
        queue_name: &str
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cfg = PoolConfig::default();
        cfg.url = Some(url.to_string());
        
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

        let client = Self {
            pool,
            queue_name: queue_name.to_string(),
            url: url.to_string(),
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
