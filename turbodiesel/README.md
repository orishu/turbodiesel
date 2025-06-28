# TurboDiesel

**TurboDiesel** is a Rust library that extends [Diesel](https://diesel.rs) with robust, ergonomic, and testable caching capabilities. It integrates seamlessly with your Diesel queries to provide fast, consistent read performance by using Redis or any other compatible cache backends, while preserving transactional consistency in relational databases like Postgres.

## Why TurboDiesel?

Many production applications combine a relational store (e.g., Postgres) with a caching layer (e.g., Redis) to reduce database load and improve query latency. However, wiring this caching logic cleanly into your Diesel queries is often repetitive, hard to test, and easy to get wrong. TurboDiesel bridges this gap by introducing composable query wrappers that transparently coordinate with your cache.

## System Design

TurboDiesel is designed to work inside your Rust application, coordinating between:

- A **relational database** (e.g., Postgres) as the source of truth
- A **cache deployment** (typically Redis) as a high-speed key-value store

The pattern looks like this:

            +----------------------------------+
            |        Your Rust App             |
            |   +--------------------------+   |
            |   |      TurboDiesel         |   |
            |   +------------+-------------+   |
            |                |                 |
            +----------------|-----------------+
                             |
               +-------------+--------------+
               |                            |
     +——————---+--------+          +––––––––+-------+
     |      Redis       |          |   Postgres     |
     |   (cache store)  |          | (source of     |
     |                  |          |   truth)       |
     +——————------------+          +–––––––--------–+

TurboDiesel enables:
- **Read-through caching** (first try cache, then fall back to DB)
- **Write-through or write-around** caching patterns (invalidate keys on updates)
- **Transparent cache population** while reading query results

All through Diesel’s fluent query interface.

## Features

✅ Populate cache automatically on query loads

✅ Read-through cache with automatic fallback

✅ Multi-key cache reads for batched queries

✅ Cache invalidation after database updates

✅ Flexible Redis or in-memory backends (via `CacheHandle` trait)

✅ Idiomatic Diesel query extensions

✅ Easily testable in unit and integration tests

## Code Examples

Here are a few highlights from the integration tests.

**Populate cache when loading records:**

```rust
let row_with_cache_key = (
    Student::as_select(),
    sql::<Text>("'student:' || id")
);

let students = students::dsl::students
    .select(row_with_cache_key)
    .populate_cache::<Student>(handle.clone())
    .load_iter::<Student, DefaultLoadingMode>(connection)?
    .map(|res| res.unwrap())
    .collect::<Vec<_>>();
```

**Read through cache for a specific key:**

```rust
let students = students::dsl::students
    .select(Student::as_select())
    .filter(students::dsl::id.eq(3))
    .try_from_cache::<Student>(handle.clone(), "student:3")
    .load_iter::<Student, DefaultLoadingMode>(connection)?
    .map(|res| res.unwrap())
    .collect::<Vec<_>>();
```

**Invalidate cache after an update:**

```rust
diesel::update(students::table)
    .set(students::dsl::name.eq("Ori2"))
    .filter(students::dsl::id.eq(2))
    .invalidate_key(handle.clone(), "student:2")
    .execute(connection)?;
```

**Combine populate + try_from_cache:**

```rust
let students = students::dsl::students
    .select(row_with_cache_key)
    .populate_cache::<Student>(handle.clone())
    .try_from_cache::<Student>(handle.clone(), "student:2")
    .load_iter::<Student, DefaultLoadingMode>(connection)?
    .map(|res| res.unwrap())
    .collect::<Vec<_>>();
```
