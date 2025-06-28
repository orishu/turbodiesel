use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug)]
pub struct CacheError {
    message: String,
    cause: Option<Box<dyn std::error::Error>>,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CacheError: {}", self.message)?;
        match &self.cause {
            Some(cause) => write!(f, " Caused by: {}", cause),
            None => Ok(()),
        }
    }
}

impl std::error::Error for CacheError {}

impl CacheError {
    pub fn new(message: &str) -> Self {
        CacheError {
            message: message.to_string(),
            cause: None,
        }
    }

    pub fn with_cause<E: std::error::Error + 'static>(message: &str, cause: E) -> Self {
        CacheError {
            message: message.to_string(),
            cause: Some(Box::new(cause)),
        }
    }
}

pub trait CacheHandle: Clone {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Result<Option<V>, CacheError>;
    fn put<V: Serialize + DeserializeOwned>(
        &mut self,
        key: &String,
        value: &V,
    ) -> Result<(), CacheError>;
    fn delete(&mut self, key: &String) -> Result<(), CacheError>;
    fn scan_keys(&self, pattern: &str) -> Result<HashMap<String, String>, CacheError>;
}

#[derive(Debug)]
pub struct HashmapCache {
    map: Rc<RefCell<HashMap<String, String>>>,
}

impl HashmapCache {
    pub fn new() -> Self {
        HashmapCache {
            map: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn handle(&self) -> HashmapCacheHandle {
        HashmapCacheHandle {
            map: Rc::clone(&self.map),
        }
    }
}

pub struct HashmapCacheHandle {
    map: Rc<RefCell<HashMap<String, String>>>,
}

impl CacheHandle for HashmapCacheHandle {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Result<Option<V>, CacheError> {
        let map = self.map.borrow();
        let value = map.get(key);
        match value {
            Some(v) => serde_json::from_str::<V>(v.as_str())
                .map(|x| Some(x))
                .map_err(|e| CacheError::with_cause("Failed to deserialize value", e)),
            None => Ok(None),
        }
    }

    fn put<V: Serialize + DeserializeOwned>(
        &mut self,
        key: &String,
        value: &V,
    ) -> Result<(), CacheError> {
        self.map.borrow_mut().insert(
            key.clone(),
            serde_json::to_string(value)
                .map_err(|e| CacheError::with_cause("Failed to serialize value", e))?,
        );
        Ok(())
    }

    fn delete(&mut self, key: &String) -> Result<(), CacheError> {
        self.map.borrow_mut().remove(key);
        Ok(())
    }

    fn scan_keys(&self, pattern: &str) -> Result<HashMap<String, String>, CacheError> {
        let wild = wildmatch::WildMatch::new(pattern);
        Ok(self
            .map
            .borrow()
            .iter()
            .filter(|(k, _)| wild.matches(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<String, String>>())
    }
}

impl Clone for HashmapCacheHandle {
    fn clone(&self) -> Self {
        HashmapCacheHandle {
            map: Rc::clone(&self.map),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_cache_put_and_get() {
        let cache = HashmapCache::new();
        let mut handle = cache.handle();

        // Define a key and value to be used in the test
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        // Put the item into the cache
        handle.put(&key, &value).expect("Failed to put value into cache");

        // Get the item from the cache
        let retrieved_value = handle.get(&key).expect("Failed to get value from cache");

        // Assert that the retrieved value matches the expected value
        assert_eq!(retrieved_value, Some(value));

        let non_existing_key = "other_key".to_string();
        let retrieved_not_found = handle
            .get::<String>(&non_existing_key)
            .expect("Failed to get value from cache");

        assert_eq!(retrieved_not_found, None);
    }
}
