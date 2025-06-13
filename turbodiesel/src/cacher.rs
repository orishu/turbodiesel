use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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

pub trait CacheHandle : Clone {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Option<V>;
    fn put<V: Serialize + DeserializeOwned>(&mut self, key: &String, value: &V);
    fn delete(&mut self, key: &String);
}

impl CacheHandle for HashmapCacheHandle {
    fn get<V: Serialize + DeserializeOwned>(&self, key: &String) -> Option<V> {
        self.map.borrow().get(key).map(|v| {
            serde_json::from_str(v.as_str())
                .unwrap_or_else(|_| panic!("Failed to deserialize value for key: {}", key))
        })
    }

    fn put<V: Serialize + DeserializeOwned>(&mut self, key: &String, value: &V) {
        self.map.borrow_mut().insert(
            key.clone(),
            serde_json::to_string(value)
                .unwrap_or_else(|_| panic!("Failed to serialize value for key: {}", key)),
        );
    }

    fn delete(&mut self, key: &String) {
        self.map.borrow_mut().remove(key);
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
        handle.put(&key, &value);

        // Get the item from the cache
        let retrieved_value = handle.get(&key);

        // Assert that the retrieved value matches the expected value
        assert_eq!(retrieved_value, Some(value));

        let non_existing_key = "other_key".to_string();
        let retrieved_not_found = handle.get::<String>(&non_existing_key);

        assert_eq!(retrieved_not_found, None);
    }
}
