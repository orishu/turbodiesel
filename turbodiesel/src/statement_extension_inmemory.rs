use diesel::query_builder::{SelectStatement, UpdateStatement};
use diesel::QuerySource;
use crate::statement_wrappers::{WrappableUpdate, WrappableQuery};
use crate::cacher::StringCache;

impl<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking> WrappableQuery
    for SelectStatement<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
{
    type Cache = StringCache;
}

impl<T, U, V, Ret> WrappableUpdate for UpdateStatement<T, U, V, Ret>
where
    T: QuerySource,
{
    type Cache = StringCache;
}
