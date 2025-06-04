use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

#[derive(Debug)]
pub struct Cache<K: Eq + Hash, V> {
    map: HashMap<K, V>,
}

pub type StringCache = Cache<String, String>;

pub trait Cacher {
    type Key: Eq + Hash;
    type Value;

    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
    fn put(&mut self, key: Self::Key, value: Self::Value);
}

impl<K: Eq + Hash, V> Cache<K, V> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new() -> Cache<K, V> {
        Cache {
            map: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash, V> Cacher for Cache<K, V> {
    type Key = K;
    type Value = V;

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }
    fn put(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub trait CachingStrategy {
    type Item: Serialize + DeserializeOwned;

    fn put_in_cache(&self, key: String, value: String);

    fn get_from_cache(&self, key: &String) -> Option<String>;

    fn put_item(&self, key: &String, item: &Self::Item) {
        self.put_in_cache(key.clone(), serde_json::to_string(item).unwrap());
    }

    fn get_item(&self, key: &String) -> Option<Self::Item> {
        self.get_from_cache(key)
            .map(|s| serde_json::from_str(s.as_str()).unwrap())
    }
}

pub struct InMemoryCachingStrategy<U>
where
    U: Serialize + DeserializeOwned,
{
    cache: Rc<RefCell<StringCache>>,
    phantom_data: PhantomData<U>,
}

impl<U> InMemoryCachingStrategy<U>
where
    U: Serialize + DeserializeOwned,
{
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(cache: Rc<RefCell<StringCache>>) -> Self {
        Self {
            cache,
            phantom_data: PhantomData,
        }
    }
}

impl<U> CachingStrategy for InMemoryCachingStrategy<U>
where
    U: Serialize + DeserializeOwned,
{
    type Item = U;

    fn put_in_cache(&self, key: String, value: String) {
        let mut c = self.cache.borrow_mut();
        c.put(key, value);
    }

    fn get_from_cache(&self, key: &String) -> Option<String> {
        self.cache.borrow().get(key).map(|x| x.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_cache_put_and_get() {
        // Create an instance of StringCache
        let cache = Rc::new(RefCell::new(StringCache::new()));

        // Create an instance of InMemoryCachingStrategy using the StringCache
        let caching_strategy = InMemoryCachingStrategy::new(cache.clone());

        // Define a key and value to be used in the test
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        // Put the item into the cache
        caching_strategy.put_item(&key, &value);

        // Get the item from the cache
        let retrieved_value = caching_strategy.get_item(&key);

        // Assert that the retrieved value matches the expected value
        assert_eq!(retrieved_value, Some(value));

        let non_existing_key = "other_key".to_string();
        let retrieved_not_found = caching_strategy.get_item(&non_existing_key);

        assert_eq!(retrieved_not_found, None);
    }
}
