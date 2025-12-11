use redis::{Client, Commands, Connection, ConnectionLike, RedisError};
use serde::{Deserialize, Serialize};

pub struct RedisClient {
    url: String,
    connection: Connection,
}

impl Clone for RedisClient {
    fn clone(&self) -> Self {
        Self::new(&self.url, 0).expect("Failed to clone Redis client")
    }
}

impl RedisClient {
    pub fn new(url: &str, database: u8) -> Result<Self, RedisError> {
        let mut client = RedisClient {
            url: url.to_string(),
            connection: Self::connect(url)?,
        };
        client.select_database(database)?;
        log::info!("Successfully connected to Redis on database {}", database);
        Ok(client)
    }

    fn connect(url: &str) -> Result<Connection, RedisError> {
        let client = Client::open(url)?;
        client.get_connection().map_err(|e| {
            log::error!("Failed to acquire connection to Redis: {}", e);
            e
        })
    }

    fn ensure_connection(&mut self) -> Result<(), RedisError> {
        if !self.connection.check_connection() {
            log::warn!("Redis connection lost, attempting to reconnect...");
            self.connection = Self::connect(&self.url)?;
            log::info!("Redis connection reestablished");
        }
        Ok(())
    }

    pub fn select_database(&mut self, database: u8) -> redis::RedisResult<()> {
        redis::cmd("SELECT")
            .arg(database)
            .query(&mut self.connection)
    }

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

    pub fn delete(&mut self, key: &str) -> redis::RedisResult<()> {
        self.ensure_connection()?;
        self.connection.del(key)
    }

    pub fn exists(&mut self, key: &str) -> redis::RedisResult<bool> {
        self.ensure_connection()?;
        self.connection.exists(key)
    }

    pub fn close(self) {
        log::info!("Redis connection closed");
    }
}
