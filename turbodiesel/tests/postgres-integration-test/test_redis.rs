use turbodiesel::{cacher::Cacher, redis_cacher::RedisCache};

#[test]
fn set_and_get() {
    let redis_url = "redis://localhost:6379";
    let mut cacher = RedisCache::new(redis_url).expect("Failed to create RedisCacher");
    let key = "test_key";
    let value = "test_value";
    cacher.put(key.to_string(), value.to_string());
    let retrieved_value: Option<String> = cacher.get(&key.to_string());
    assert_eq!(
        retrieved_value,
        Some(value.to_string()),
        "Retrieved value does not match set value"
    );
}
