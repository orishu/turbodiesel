use turbodiesel::{cacher::CacheHandle, redis_cacher::RedisCache};

#[test]
fn set_and_get() {
    let redis_url = "redis://localhost:6379";
    let cache = RedisCache::new(redis_url).expect("Failed to create RedisCacher");
    let mut handle = cache.handle();

    let key = "test_key".to_string();
    let value = "test_value".to_string();
    handle.raw_delete(&key);
    handle
        .put(&key, &value)
        .expect("Failed to put value into cache");
    let retrieved_value: Option<String> = handle.get(&key).expect("Failed to get value from cache");
    assert_eq!(
        retrieved_value,
        Some(value.to_string()),
        "Retrieved value does not match set value"
    );
}
