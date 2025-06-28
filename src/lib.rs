//! # turbodiesel
//!
//! `turbodiesel` extends Diesel ORM with convenient, opt-in caching primitives to improve the performance of repeated reads,
//! while preserving transactional consistency in Postgres-backed systems.
//!
//! It introduces a family of *statement wrappers* that allow caching behaviors to be applied transparently to Diesel query builders:
//!
//! - `populate_cache`: executes the query and populates a cache with the results
//! - `try_from_cache`: attempts to load from cache first, falling back to the database if the key is missing
//! - `try_from_cache_multi`: same as `try_from_cache` but supports multiple keys at once
//! - `try_from_cache_and_populate`: first attempts cache lookup, then falls back to DB if missing, and updates the cache afterward
//! - `invalidate_key`: invalidates a specific cache key in a single Diesel update statement
//!
//! The design supports both in-memory and Redis-backed cache handles, providing flexibility for unit tests and production environments.
//!
//! These primitives integrate directly into Diesel’s query DSL with minimal friction, while allowing fine-grained control
//! over cache population, invalidation, and fallback behavior. They enable safe, testable caching around Diesel’s transactional
//! operations, improving read performance while remaining consistent with your relational schema.
//!
//! Typical usage patterns include populating the cache on bulk loads, invalidating cache entries on updates, and verifying
//! cache coherence under concurrent conditions, as demonstrated in the included integration tests.
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
pub mod redis_test_util;
pub mod postgres_test_util;
