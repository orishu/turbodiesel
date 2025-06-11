use crate::cacher::Cacher;
use itertools::process_results;
use redis;
use redis::Commands;
use redis::RedisError;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;
use std::collections::HashMap;

pub struct RedisCache {
    client: redis::Client,
}

impl RedisCache {
    pub fn new(redis_url: &str) -> Result<Self, RedisError> {
        let client = redis::Client::open(redis_url)?;
        Ok(RedisCache { client })
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, RedisError> {
        let mut con = self.client.get_connection()?;
        match con.get::<_, Option<String>>(key)? {
            Some(value) => Ok(Some(serde_json::from_str(&value)?)),
            None => Ok(None),
        }
    }

    pub fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<(), redis::RedisError> {
        let mut con = self.client.get_connection()?;
        let value_str = serde_json::to_string(value)?;
        con.set::<&str, String, ()>(key, value_str)?;
        Ok(())
    }

    pub fn scan_keys(&self, pattern: &str) -> Result<HashMap<String, String>, RedisError> {
        let mut con = self.client.get_connection()?;
        let keys: Vec<String> = con.keys(pattern)?;

        process_results(
            keys.into_iter()
                .map(|k| Ok((k.clone(), con.get::<_, Option<String>>(k)?.unwrap()))),
            |iter| iter.collect(),
        )
    }
}

impl Cacher for RedisCache {
    type Key = String;
    type Value = String;

    fn get(&self, key: &String) -> Option<String> {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.get::<_, Option<String>>(key)
            .expect("Failed to get value from Redis")
    }

    fn put(&mut self, key: String, value: String) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.set::<&str, String, ()>(&key, value)
            .expect("Failed to set value in Redis");
    }

    fn delete(&mut self, key: String) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.del::<&str, ()>(&key)
            .expect("Failed to delete key from Redis");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_get_and_set() {
        let redis_url = "redis://localhost:6379";
        let mut cacher = RedisCache::new(redis_url).expect("Failed to create RedisCache");
        let key = "test_key";
        let value = "test_value";

        // Test set
        cacher
            .set(key, &value)
            .expect("Failed to set value in Redis");

        // Test get
        let retrieved_value: Option<String> =
            cacher.get(key).expect("Failed to get value from Redis");
        assert_eq!(
            retrieved_value,
            Some(value.to_string()),
            "Retrieved value does not match set value"
        );

        // Clean up
        cacher.delete(key.to_string());
    }
}
