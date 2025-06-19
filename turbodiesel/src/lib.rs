
pub mod cacher;
pub mod redis_cacher;
pub mod statement_wrappers;

#[cfg(all(feature = "inmemory", feature = "redis"))]
compile_error!("feature \"inmemory\" and feature \"redis\" cannot be enabled at the same time");

#[cfg(feature = "inmemory")]
pub mod statement_extension_inmemory;

#[cfg(feature = "redis")]
pub mod statement_extension_redis;

pub mod test_utils;