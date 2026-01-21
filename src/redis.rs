use redis::{Client, Commands, Connection, ConnectionLike, RedisError};
use serde::{Deserialize, Serialize};

/// A high-level Redis client with automatic connection management and JSON serialization.
///
/// This client handles connecting to Redis, switching databases, and provides
/// type-safe wrappers for common operations by automatically serializing/deserializing
/// data to/from JSON.
pub struct RedisClient {
    url: String,
    connection: Connection,
}

impl Clone for RedisClient {
    /// Creates a new connection to the same Redis instance.
    fn clone(&self) -> Self {
        Self::new(&self.url, 0).expect("Failed to clone Redis client")
    }
}

impl RedisClient {
    /// Establishes a new connection to Redis and selects the specified database.
    pub fn new(url: &str, database: u8) -> Result<Self, RedisError> {
        let mut client = RedisClient {
            url: url.to_string(),
            connection: Self::connect(url)?,
        };
        client.select_database(database)?;
        log::info!("Successfully connected to Redis on database {}", database);
        Ok(client)
    }

    /// Internal helper to open a connection.
    fn connect(url: &str) -> Result<Connection, RedisError> {
        let client = Client::open(url)?;
        client.get_connection().map_err(|e| {
            log::error!("Failed to acquire connection to Redis: {}", e);
            e
        })
    }

    /// Checks if the current connection is still alive and attempts to reconnect if not.
    ///
    /// This is called before every operation to ensure high availability and
    /// resilience against transient network failures.
    fn ensure_connection(&mut self) -> Result<(), RedisError> {
        if !self.connection.check_connection() {
            log::warn!("Redis connection lost, attempting to reconnect...");
            self.connection = Self::connect(&self.url)?;
            log::info!("Redis connection reestablished");
        }
        Ok(())
    }

    /// Switches the current connection to a different database index.
    pub fn select_database(&mut self, database: u8) -> redis::RedisResult<()> {
        redis::cmd("SELECT")
            .arg(database)
            .query(&mut self.connection)
    }

    /// Stores a serializable value in Redis.
    ///
    /// The value is converted to a JSON string before storage.
    pub fn set<T: Serialize>(&mut self, key: &str, value: &T) -> redis::RedisResult<()> {
        self.ensure_connection()?;
        let serialized = serde_json::to_string(value).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.connection.set(key, serialized)
    }

    /// Stores a serializable value in Redis with a Time-To-Live (TTL) in seconds.
    ///
    /// The value is converted to a JSON string before storage.
    pub fn set_ex<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
        ttl: u64,
    ) -> redis::RedisResult<()> {
        self.ensure_connection()?;
        let serialized = serde_json::to_string(value).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.connection.set_ex(key, serialized, ttl)
    }

    /// Retrieves and deserializes a value from Redis.
    ///
    /// Returns `Ok(Some(T))` if the key exists and can be deserialized,
    /// `Ok(None)` if the key doesn't exist, or an error if deserialization fails.
    pub fn get<T: for<'de> Deserialize<'de>>(
        &mut self,
        key: &str,
    ) -> redis::RedisResult<Option<T>> {
        self.ensure_connection()?;
        let value: Option<String> = self.connection.get(key)?;
        match value {
            Some(v) => serde_json::from_str(&v).map(Some).map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Deserialization error",
                    e.to_string(),
                ))
            }),
            None => Ok(None),
        }
    }

    /// Deletes a key from Redis.
    pub fn delete(&mut self, key: &str) -> redis::RedisResult<()> {
        self.ensure_connection()?;
        self.connection.del(key)
    }

    /// Checks if a key exists in Redis.
    pub fn exists(&mut self, key: &str) -> redis::RedisResult<bool> {
        self.ensure_connection()?;
        self.connection.exists(key)
    }

    /// Logs that the connection is closing.
    pub fn close(self) {
        log::info!("Redis connection closed");
    }
}
