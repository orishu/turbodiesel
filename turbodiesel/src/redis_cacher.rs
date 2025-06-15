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
        println!("Loaded Redis functions for module: {:?}", response);
        Ok(())
    }
}

impl CacheHandle for RedisCacheHandle {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Option<V> {
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
        println!("Response from Redis td_get function call: {:?}", response);
        match response {
            redis::Value::Nil => return None,
            redis::Value::SimpleString(str_value) => {
                let value: V = serde_json::from_str(str_value.as_str()).ok()?;
                return Some(value);
            }
            redis::Value::BulkString(data) => {
                let str_value = String::from_utf8(data).ok()?;
                let value: V = serde_json::from_str(&str_value).ok()?;
                return Some(value);
            }
            _ => panic!("Unexpected response type from Redis function call"),
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
        println!("Response from Redis td_set function call: {:?}", response);
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
        println!(
            "Response from Redis td_invalidate function call: {:?}",
            response
        );
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

            // Test delete
            handle.delete(&key);

            // Test get
            let empty: Option<String> = handle.get(&key);
            assert_eq!(
                empty, None,
                "Retrieved value expected to be None after deletion"
            );
        });
        println!("Redis integration test completed successfully.");
    }
}
