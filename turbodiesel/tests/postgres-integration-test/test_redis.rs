use turbodiesel::redis_cacher::RedisCache;

#[test]
fn set_and_get() {
    let redis_url = "redis://localhost:6379";
    let cacher = RedisCache::new(redis_url).expect("Failed to create RedisCacher");
    let key = "test_key";
    let value = "test_value";
    cacher.set(key, &value).expect("Failed to set value in Redis");
    let retrieved_value: Option<String> = cacher.get(key).expect("Failed to get value from Redis");
    assert_eq!(retrieved_value, Some(value.to_string()), "Retrieved value does not match set value");
}