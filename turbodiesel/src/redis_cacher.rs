use crate::cacher::CacheHandle;
use async_std::task;
use itertools::process_results;
use redis;
use redis::Commands;
use redis::RedisError;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

pub struct RedisCache {
    client: redis::Client,
}

impl RedisCache {
    pub fn new(redis_url: &str) -> Result<Self, RedisError> {
        let client = redis::Client::open(redis_url)?;
        Ok(RedisCache { client })
    }

    pub fn handle(&self) -> RedisCacheHandle {
        RedisCacheHandle::new(self.client.clone())
    }
}

pub struct RedisCacheHandle {
    client: redis::Client,
}

impl RedisCacheHandle {
    pub fn new(client: redis::Client) -> Self {
        RedisCacheHandle { client }
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

    pub fn check_online(&self) -> Result<(), RedisError> {
        let mut con = self.client.get_connection()?;
        con.ping::<String>()?;
        Ok(())
    }

    pub async fn wait_until_online(&self, retries: usize) -> Result<(), RedisError> {
        for _ in 0..retries {
            if self.check_online().is_ok() {
                return Ok(());
            }
            task::sleep(Duration::from_secs(1)).await;
        }
        Err(RedisError::from((
            redis::ErrorKind::IoError,
            "Redis is not online",
        )))
    }
}

impl CacheHandle for RedisCacheHandle {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Option<V> {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.get::<_, Option<String>>(key)
            .expect("Failed to get value from Redis")
            .and_then(|v| {
                let value = serde_json::from_str(v.as_str()).ok();
                value.map(|v: serde_json::Value| {
                    let untyped_val = v.get("value".to_string()).unwrap().clone();
                    serde_json::from_value(untyped_val).unwrap()
                })
            })
    }

    fn put<V: Serialize + DeserializeOwned>(&mut self, key: &String, value: &V) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let json_value = serde_json::json!({
            "value": value,
            "timestamp": now,
        });
        con.set::<&str, String, ()>(&key, serde_json::to_string(&json_value).unwrap())
            .expect("Failed to set value in Redis");
    }

    fn delete(&mut self, key: &String) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.del::<&str, ()>(key)
            .expect("Failed to delete key from Redis");
    }
}

impl Clone for RedisCacheHandle {
    fn clone(&self) -> Self {
        RedisCacheHandle {
            client: self.client.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use dockertest::{DockerTest, TestBodySpecification};
    use dotenvy::dotenv;

    use super::*;

    #[test]
    fn test_redis_get_and_set() {
        dotenv().ok();
        let image =
            dockertest::Image::with_repository("redis").source(dockertest::Source::DockerHub);
        let mut redis_container = TestBodySpecification::with_image(image);
        redis_container.modify_port_map(6379, 6380);
        let mut test = DockerTest::new();
        test.provide_container(redis_container);
        println!("Running Redis integration test...");
        test.run(|_| async move {
            let redis_url = "redis://localhost:6380";
            let cache = RedisCache::new(redis_url).expect("Failed to create RedisCache");
            let mut handle = cache.handle();
            handle
                .wait_until_online(6)
                .await
                .expect("Redis is not online after retries");

            let key = "test_key".to_string();
            let value = "test_value".to_string();

            // Test put
            handle.put(&key, &value);

            // Test get
            let retrieved_value: Option<String> = handle.get(&key);
            assert_eq!(
                retrieved_value,
                Some(value),
                "Retrieved value does not match set value"
            );

            // Clean up
            handle.delete(&key);
        });
        println!("Redis integration test completed successfully.");
    }
}
