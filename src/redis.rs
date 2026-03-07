use redis::{Client, Commands, Connection, ConnectionLike, RedisError};
use serde::{Deserialize, Serialize};

const MAX_RETRY_ATTEMPTS: u32 = 10;
const INITIAL_RETRY_DELAY_SECS: u64 = 3;
const MAX_RETRY_DELAY_SECS: u64 = 30;

/// Strips credentials from a URL for safe logging (e.g. `redis://user:pass@host` → `redis://***@host`).
fn sanitize_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            return format!("{}://***@{}", &url[..scheme_end], &after_scheme[at_pos + 1..]);
        }
    }
    url.to_string()
}

/// A synchronous Redis client with automatic connection management and JSON serialization.
///
/// This client handles connecting to Redis, switching databases, and provides
/// type-safe wrappers for common operations by automatically serializing/deserializing
/// data to/from JSON.
///
/// # Blocking Operations
///
/// All operations on this client are **synchronous and will block the calling thread**.
/// When using this client from an async context (e.g. inside a Tokio runtime), wrap
/// calls in [`tokio::task::spawn_blocking`] to avoid blocking the async executor:
///
/// ```ignore
/// let value = tokio::task::spawn_blocking(move || {
///     redis_client.get::<MyStruct>("key")
/// }).await??;
/// ```
pub struct RedisClient {
    url: String,
    database: u8,
    connection: Connection,
}

impl RedisClient {
    /// Establishes a new connection to Redis and selects the specified database.
    ///
    /// Retries up to 10 times with exponential backoff (3s → 6s → 12s, capped at 30s)
    /// before returning an error.
    pub fn new(url: &str, database: u8) -> Result<Self, RedisError> {
        let connection = Self::connect_with_retry(url, database)?;
        log::info!("Successfully connected to Redis on database {}", database);
        Ok(RedisClient {
            url: url.to_string(),
            database,
            connection,
        })
    }

    /// Creates a new client connected to the same Redis instance and database.
    ///
    /// Unlike `Clone`, this method is fallible and will return an error if the
    /// connection cannot be established.
    pub fn try_clone(&self) -> Result<Self, RedisError> {
        Self::new(&self.url, self.database)
    }

    /// Internal helper to open a connection and select database.
    fn connect_and_select(url: &str, database: u8) -> Result<Connection, RedisError> {
        let client = Client::open(url)?;
        let mut conn = client.get_connection().map_err(|e| {
            log::error!("Failed to acquire connection to Redis at {}: {}", sanitize_url(url), e);
            e
        })?;
        redis::cmd("SELECT").arg(database).query::<()>(&mut conn)?;
        Ok(conn)
    }

    /// Connects to Redis with bounded retry logic and exponential backoff.
    ///
    /// Retries up to `MAX_RETRY_ATTEMPTS` times with delays of 3s, 6s, 12s, ...
    /// capped at `MAX_RETRY_DELAY_SECS`. Returns an error if all attempts fail.
    fn connect_with_retry(url: &str, database: u8) -> Result<Connection, RedisError> {
        let mut attempt = 1u32;
        let mut delay = INITIAL_RETRY_DELAY_SECS;
        loop {
            match Self::connect_and_select(url, database) {
                Ok(conn) => {
                    if attempt > 1 {
                        log::info!("Redis reconnected after {} attempts", attempt);
                    }
                    return Ok(conn);
                }
                Err(e) => {
                    if attempt >= MAX_RETRY_ATTEMPTS {
                        log::error!(
                            "Redis connection to {} failed after {} attempts: {}",
                            sanitize_url(url), attempt, e
                        );
                        return Err(e);
                    }
                    if attempt == 1 {
                        log::warn!(
                            "Redis connection to {} failed: {}. Retrying (max {} attempts)...",
                            sanitize_url(url), e, MAX_RETRY_ATTEMPTS
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_secs(delay));
                    delay = (delay * 2).min(MAX_RETRY_DELAY_SECS);
                    attempt += 1;
                }
            }
        }
    }

    /// Checks if the current connection is still alive and attempts to reconnect if not.
    ///
    /// This is called before every operation to ensure high availability and
    /// resilience against transient network failures.
    fn ensure_connection(&mut self) -> Result<(), RedisError> {
        if !self.connection.check_connection() {
            log::warn!("Redis connection lost, attempting to reconnect...");
            self.connection = Self::connect_with_retry(&self.url, self.database)?;
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

    /// Deletes multiple keys from Redis in a single operation.
    ///
    /// # Arguments
    /// * `keys` - A slice of key names to delete.
    pub fn delete_bulk(&mut self, keys: &[&str]) -> redis::RedisResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.ensure_connection()?;
        self.connection.del(keys)
    }

    /// Retrieves all keys matching a pattern.
    ///
    /// # Arguments
    /// * `pattern` - The glob-style pattern to match (e.g., "user:*").
    pub fn keys(&mut self, pattern: &str) -> redis::RedisResult<Vec<String>> {
        self.ensure_connection()?;
        self.connection.keys(pattern)
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

    /// FOR TESTING ONLY: Forces a reconnection to Redis, simulating a connection reset.
    pub fn force_reconnect(&mut self) {
        log::info!("Forcing Redis reconnection for testing...");
        if let Ok(conn) = Self::connect_with_retry(&self.url, self.database) {
            self.connection = conn;
        }
    }
}
