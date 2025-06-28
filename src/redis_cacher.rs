use crate::cacher::CacheError;
use crate::cacher::CacheHandle;
use async_std::task;
use log::{debug, info};
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
        debug!("Response from Redis td_get function call: {:?}", response);
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
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Result<Option<V>, CacheError> {
        match self.raw_get(key) {
            Some(value) => match value {
                redis::Value::SimpleString(str_value) => {
                    let deserialized: V = serde_json::from_str(str_value.as_str())
                        .map_err(|e| CacheError::with_cause("Failed to deserialize value", e))?;
                    Ok(Some(deserialized))
                }
                redis::Value::BulkString(data) => {
                    let str_value = String::from_utf8(data).map_err(|e| {
                        CacheError::with_cause("Failed to convert bulk string to UTF-8", e)
                    })?;
                    let deserialized: V = serde_json::from_str(&str_value)
                        .map_err(|e| CacheError::with_cause("Failed to deserialize value", e))?;
                    Ok(Some(deserialized))
                }
                redis::Value::Nil => Ok(None),
                _ => panic!("Unexpected response type from Redis function call"),
            },
            None => Ok(None),
        }
    }

    fn put<V: Serialize + DeserializeOwned>(
        &mut self,
        key: &String,
        value: &V,
    ) -> Result<(), CacheError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| CacheError::with_cause("Failed to connect to Redis", e))?;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| CacheError::with_cause("Failed to get current time", e))?;
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
        .map_err(|e| CacheError::with_cause("Failed to call Redis td_set function", e))?;
        let response = con.recv_response().map_err(|e| {
            CacheError::with_cause("Failed to receive response from Redis function call", e)
        })?;
        debug!("Response from Redis td_set function call: {:?}", response);
        Ok(())
    }

    fn delete(&mut self, key: &String) -> Result<(), CacheError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| CacheError::with_cause("Failed to connect to Redis", e))?;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| CacheError::with_cause("Failed to get current time", e))?;
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
        .map_err(|e| CacheError::with_cause("Failed to call Redis td_invalidate function", e))?;
        let response = con.recv_response().map_err(|e| {
            CacheError::with_cause("Failed to receive response from Redis function call", e)
        })?;
        debug!(
            "Response from Redis td_invalidate function call: {:?}",
            response
        );
        Ok(())
    }

    fn scan_keys(&self, pattern: &str) -> Result<HashMap<String, String>, CacheError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| CacheError::with_cause("Failed to connect to Redis", e))?;
        let keys: Vec<String> = con
            .keys(pattern)
            .map_err(|e| CacheError::with_cause("Failed to scan keys", e))?;

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
    use crate::redis_test_util::RedisTestUtil;

    use super::*;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        crate::test_utils::init_logging_for_tests();
    }

    #[tokio::test]
    async fn test_redis_get_and_set() {
        let redis_test = RedisTestUtil::new();
        redis_test
            .run_test_with_redis(async move |redis_url, _| {
                let cache =
                    RedisCache::new(redis_url.as_str()).expect("Failed to create RedisCache");
                let mut handle = cache.handle();

                let key = "test_key".to_string();
                let value = "test_value".to_string();

                // Test put
                handle
                    .put(&key, &value)
                    .expect("Failed to put value into cache");

                // Test get
                let retrieved_value: Option<String> =
                    handle.get(&key).expect("Failed to get value from cache");
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
                handle
                    .delete(&key)
                    .expect("Failed to delete key from cache");

                // Test get
                let empty: Option<String> =
                    handle.get(&key).expect("Failed to get value from cache");
                assert_eq!(
                    empty, None,
                    "Retrieved value expected to be None after deletion"
                );
            })
            .await;
    }
}
