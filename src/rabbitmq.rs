use deadpool_lapin::{Config as PoolConfig, Pool, Runtime};
use deadpool_lapin::lapin::{
    message::Delivery,
    options::*,
    types::FieldTable,
    BasicProperties,
};
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use crate::RabbitHandler;
use std::sync::Arc;

/// An asynchronous RabbitMQ client for publishing and consuming messages.
///
/// This client uses `deadpool-lapin` for connection pooling and automatic recovery.
pub struct RabbitMQClient {
    pool: Pool,
    queue_name: String,
}

#[allow(unused)]
impl RabbitMQClient {
    /// Creates a new RabbitMQ client with connection pooling.
    pub async fn new(
        url: &str,
        queue_name: &str
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cfg = PoolConfig::default();
        cfg.url = Some(url.to_string());
        
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        
        // Ensure default queue exists
        let client = Self {
            pool,
            queue_name: queue_name.to_string(),
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
        self.ensure_queue(&queue).await?;

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
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let queue_name = self.queue_name.clone();
        let pool = self.pool.clone();
        let consumer_tag = consumer_tag.to_string();

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
                                    let payload = delivery.data.clone();
                                    match message_handler(payload).await {
                                        Ok(_) => { let _ = delivery.ack(BasicAckOptions::default()).await; }
                                        Err(e) => {
                                            log::error!("Error processing delivery: {}", e);
                                            let _ = delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await;
                                        }
                                    }
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

    async fn process_delivery<F, Fut>(
        &self,
        delivery: Delivery,
        message_handler: &F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let payload = delivery.data.clone();
        match message_handler(payload).await {
            Ok(_) => {
                delivery.ack(BasicAckOptions::default()).await?;
                Ok(())
            }
            Err(e) => {
                log::error!("Message handler failed: {}", e);
                delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await?;
                Ok(())
            }
        }
    }

    /// Publishes a serializable message.
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
                BasicProperties::default(),
            )
            .await?;

        log::info!("Published message to queue '{}'", routing);
        Ok(())
    }
}
