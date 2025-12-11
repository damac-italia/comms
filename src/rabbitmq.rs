use lapin::{
    message::Delivery,
    options::*,
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Error as LapinError, RecoveryConfig,
};
use serde::{Serialize};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

pub struct RabbitMQClient {
    channel: Channel,
    queue_name: String,
}

impl RabbitMQClient {
    pub async fn new(
        url: &str,
        queue_name: &str,
        max_retries: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let properties = ConnectionProperties::default()
            .with_connection_name("rabbitmq_client_connection".into())
            .with_experimental_recovery_config(RecoveryConfig::full())
            .configure_backoff(|backoff| {
                backoff.with_max_times(max_retries);
            });

        let connection = Connection::connect(url, properties).await?;
        log::info!("Connected to RabbitMQ at: {}", url);

        let channel = connection.create_channel().await?;
        log::info!("RabbitMQ channel created");

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

        Ok(Self {
            channel,
            queue_name: queue_name.to_string(),
        })
    }

    pub async fn start_consuming<F, Fut>(
        &mut self,
        consumer_tag: &str,
        message_handler: F,
        cancellation_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let mut consumer = self
            .channel
            .basic_consume(
                &self.queue_name,
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        log::info!(
            "Started consuming messages from queue '{}'",
            self.queue_name
        );

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    log::info!("Cancellation received, shutting down RabbitMQ consumer...");
                    break;
                }
                delivery_option = consumer.next() => {
                    match delivery_option {
                        Some(Ok(delivery)) => {
                            if let Err(e) = self.process_delivery(delivery, &message_handler).await {
                                log::error!("Error processing delivery: {}", e);
                            }
                        }
                        Some(Err(e)) => {
                            log::error!("Error receiving message: {}", e);
                            self.handle_connection_error(e).await?;
                        }
                        None => {
                            log::warn!("Consumer stream ended unexpectedly");
                            break;
                        }
                    }
                }
            }
        }

        log::info!(
            "Stopped consuming messages from queue '{}'",
            self.queue_name
        );
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
        log::info!("Received message of {} bytes", payload.len());

        match message_handler(payload).await {
            Ok(_) => self.ack_delivery(delivery).await,
            Err(e) => {
                log::error!("Message handler failed: {}", e);
                self.nack_delivery(delivery).await
            }
        }
    }

    pub async fn publish<T: Serialize>(
        &self,
        message: &T,
        routing_key: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(message)?;
        let routing = routing_key.unwrap_or(&self.queue_name);

        self.channel
            .basic_publish(
                "",
                routing,
                BasicPublishOptions::default(),
                &payload,
                lapin::BasicProperties::default(),
            )
            .await?;

        log::info!("Published message to queue '{}'", routing);
        Ok(())
    }

    async fn ack_delivery(&self, delivery: Delivery) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|e| {
                log::error!("Failed to ack message: {}", e);
                e
            })?;
        Ok(())
    }

    async fn nack_delivery(&self, delivery: Delivery) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                log::error!("Failed to nack message: {}", e);
                e
            })?;
        Ok(())
    }

    async fn handle_connection_error(
        &self,
        error: LapinError,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if error.is_amqp_soft_error() || error.is_amqp_hard_error() {
            log::warn!("Detected connection error, waiting for recovery...");
            self.channel.wait_for_recovery(error).await?;
            log::info!("Connection recovered successfully");
        }
        Ok(())
    }
}
