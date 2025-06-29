use turbodiesel::{cacher::CacheHandle, redis_cacher::RedisCache};

// This test runs against a local Redis server, it assumes that the custom functions have been loaded.
// Load the function from the lua/functions.lua file using the following command:
// cat lua/functions.lua | redis-cli -x FUNCTION LOAD REPLACE
#[test]
fn basic_get_put_invalidate() {
    let redis_url = "redis://localhost:6379";
    let cache = RedisCache::new(redis_url).expect("Failed to create RedisCacher");
    let mut handle = cache.handle();

    let key = "test_key".to_string();
    let value = "test_value".to_string();
    handle.raw_delete(&key);
    handle
        .put(&key, &value)
        .expect("Failed to put value into cache");
    let mut retrieved_value: Option<String> =
        handle.get(&key).expect("Failed to get value from cache");
    assert_eq!(
        retrieved_value,
        Some(value.to_string()),
        "Retrieved value does not match set value"
    );
    handle.delete(&key).expect("Failed to invalidate in cache");
    retrieved_value = handle.get(&key).expect("Failed to get value from cache");
    assert_eq!(
        retrieved_value, None,
        "Retrieved value is expected to be None"
    );
}
