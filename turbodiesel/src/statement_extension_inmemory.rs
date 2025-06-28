use crate::cacher::HashmapCacheHandle;
use crate::statement_wrappers::{WrappableQuery, WrappableUpdate};
use diesel::QuerySource;
use diesel::query_builder::{SelectStatement, UpdateStatement};

impl<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking> WrappableQuery
    for SelectStatement<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
{
    type Cache = HashmapCacheHandle;
}

impl<T, U, V, Ret> WrappableUpdate for UpdateStatement<T, U, V, Ret>
where
    T: QuerySource,
{
    type Cache = HashmapCacheHandle;
}

impl<T, C> WrappableQuery
    for SelectCachingWrapper<T, C>
where
    C: CacheHandle,
{
    type Cache = HashmapCacheHandle;
}
