use crate::cacher::CacheHandle;
use crate::redis_cacher::RedisCacheHandle;
use crate::statement_wrappers::{SelectCachingWrapper, WrappableQuery, WrappableUpdate};
use diesel::QuerySource;
use diesel::query_builder::{SelectStatement, UpdateStatement};

impl<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking> WrappableQuery
    for SelectStatement<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
{
    type Cache = RedisCacheHandle;
}

impl<T, U, V, Ret> WrappableUpdate for UpdateStatement<T, U, V, Ret>
where
    T: QuerySource,
{
    type Cache = RedisCacheHandle;
}

impl<T, C> WrappableQuery
    for SelectCachingWrapper<T, C>
where
    C: CacheHandle,
{
    type Cache = RedisCacheHandle;
}
