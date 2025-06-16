use crate::cacher::CacheHandle;
use async_std::task;
use log::info;
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

    pub fn load_redis_functions(&self) -> Result<(), RedisError> {
        let script = include_str!("../lua/functions.lua");
        let mut con = self.client.get_connection()?;
        con.send_packed_command(
            redis::cmd("FUNCTION")
                .arg("LOAD")
                .arg("REPLACE")
                .arg(script)
                .get_packed_command()
                .as_slice(),
        )?;
        let response = con.recv_response()?;
        info!("Loaded Redis functions for module: {:?}", response);
        Ok(())
    }

    fn raw_get(&self, key: &String) -> Option<redis::Value> {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        con.send_packed_command(
            redis::cmd("FCALL")
                .arg("td_get")
                .arg(1)
                .arg(key)
                .get_packed_command()
                .as_slice(),
        )
        .expect("Failed to call Redis function");
        let response = con
            .recv_response()
            .expect("Failed to receive response from Redis function call");
        info!("Response from Redis td_get function call: {:?}", response);
        match response {
            redis::Value::Nil => None,
            _ => Some(response),
        }
    }

    pub fn raw_delete(&mut self, key: &String) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        _ = con.del::<_, ()>(key);
    }
}

impl CacheHandle for RedisCacheHandle {
    type Error = RedisError;

    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Option<V> {
        match self.raw_get(key) {
            Some(value) => match value {
                redis::Value::SimpleString(str_value) => {
                    let deserialized: V = serde_json::from_str(str_value.as_str()).ok()?;
                    Some(deserialized)
                }
                redis::Value::BulkString(data) => {
                    let str_value = String::from_utf8(data).ok()?;
                    let deserialized: V = serde_json::from_str(&str_value).ok()?;
                    Some(deserialized)
                }
                redis::Value::Nil => None,
                _ => panic!("Unexpected response type from Redis function call"),
            },
            None => None,
        }
    }

    fn put<V: Serialize + DeserializeOwned>(&mut self, key: &String, value: &V) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        con.send_packed_command(
            redis::cmd("FCALL")
                .arg("td_set")
                .arg(1)
                .arg(key)
                .arg(serde_json::to_string(value).unwrap())
                .arg(now.as_secs())
                .arg(now.subsec_nanos())
                .get_packed_command()
                .as_slice(),
        )
        .expect("Failed to call Redis function");
        let response = con
            .recv_response()
            .expect("Failed to receive response from Redis function call");
        info!("Response from Redis td_set function call: {:?}", response);
    }

    fn delete(&mut self, key: &String) {
        let mut con = self
            .client
            .get_connection()
            .expect("Failed to connect to Redis");
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        con.send_packed_command(
            redis::cmd("FCALL")
                .arg("td_invalidate")
                .arg(1)
                .arg(key)
                .arg(now.as_secs())
                .arg(now.subsec_nanos())
                .get_packed_command()
                .as_slice(),
        )
        .expect("Failed to call Redis td_invalidate function");
        let response = con
            .recv_response()
            .expect("Failed to receive response from Redis function call");
        info!(
            "Response from Redis td_invalidate function call: {:?}",
            response
        );
    }

    fn scan_keys(&self, pattern: &str) -> Result<HashMap<String, String>, Self::Error> {
        let mut con = self.client.get_connection()?;
        let keys: Vec<String> = con.keys(pattern)?;

        Ok(keys
            .iter()
            .map(|k| (k.clone(), self.raw_get(&k)))
            .filter_map(|x| match x {
                (k, Some(v)) => Some((k, format!("{:?}", v))),
                _ => None,
            })
            .collect())
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

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        crate::test_utils::init_logging_for_tests();
    }

    #[test]
    fn test_tmp() {
        info!("ORIORI");
    }

    #[test]
    fn test_redis_get_and_set() {
        dotenv().ok();
        let image =
            dockertest::Image::with_repository("redis").source(dockertest::Source::DockerHub);
        let mut redis_container = TestBodySpecification::with_image(image);
        redis_container.modify_port_map(6379, 6380);
        let mut test = DockerTest::new();
        test.provide_container(redis_container);
        info!("Running Redis integration test...");
        test.run(|_| async move {
            let redis_url = "redis://localhost:6380";
            let cache = RedisCache::new(redis_url).expect("Failed to create RedisCache");
            let mut handle = cache.handle();
            handle
                .wait_until_online(6)
                .await
                .expect("Redis is not online after retries");
            handle
                .load_redis_functions()
                .expect("Failed to load Redis functions");

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

            // Test scan keys
            let scan_result = handle.scan_keys("test_key*").expect("Failed to scan keys");
            assert_eq!(scan_result.len(), 1, "Expected one key in scan result");
            let expected_raw_value = "bulk-string('\"\\\"test_value\\\"\"')".to_string();
            assert_eq!(
                scan_result.get(&"test_key".to_string()),
                Some(&expected_raw_value),
                "Scan result does not match expected value"
            );

            // Test delete
            handle.delete(&key);

            // Test get
            let empty: Option<String> = handle.get(&key);
            assert_eq!(
                empty, None,
                "Retrieved value expected to be None after deletion"
            );
        });
        info!("Redis integration test completed successfully.");
    }
}
