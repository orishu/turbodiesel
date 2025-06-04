use diesel::dsl::sql;
use diesel::expression::QueryMetadata;
use diesel::pg;
use diesel::query_builder::{SelectClauseExpression, SelectQuery};
use diesel::sql_types::SqlType;
use diesel::sql_types::Text;
use diesel::{Queryable, Selectable};
use postgres::Client;
use postgres::NoTls;
use postgres::fallible_iterator::FallibleIterator;

use diesel::pg::data_types::PgDate;
use diesel::prelude::*;
use dotenvy::dotenv;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::iter::{Inspect, Map};
use std::marker::PhantomData;
use std::rc::Rc;

use diesel::backend::Backend;
use diesel::connection::{Connection, DefaultLoadingMode, LoadConnection};
use diesel::expression::SelectableExpression;
use diesel::helper_types::Limit;
use diesel::query_builder::{AsQuery, SelectStatement};
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl, methods};
use diesel::result::Error;
use diesel::result::QueryResult;
use julian::{Calendar, Month, system2jdn};

use crate::cacher::*;

use serde::{Deserialize, Serialize};

pub struct ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<(U, String)>>,
    C: CachingStrategy<Item = U>,
    U: Serialize,
{
    inner: I,
    caching_strategy: C,
}

impl<I, U, C> Iterator for ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<(U, String)>>,
    C: CachingStrategy<Item = U>,
    U: Serialize + std::fmt::Debug,
{
    type Item = QueryResult<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(ref it_res) = item {
            println!("Item result is {:?}", it_res);
            if let Ok(it) = it_res {
                self.caching_strategy.put_item(&it.1, &it.0);
                println!("Item cached (2)");
            }
        }
        item.map(|r| r.map(|pair| pair.0))
    }
}

pub struct ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CachingStrategy<Item = U>,
    U: Serialize + DeserializeOwned,
    K: Iterator<Item = String>,
{
    inner: I,
    keys: K,
    caching_strategy: C,
}

impl<I, U, C, K> ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CachingStrategy<Item = U>,
    U: Serialize + DeserializeOwned,
    K: Iterator<Item = String>,
{
    fn new(inner: I, caching_strategy: C, keys: K) -> Self {
        Self {
            inner,
            keys,
            caching_strategy,
        }
    }
}

impl<I, U, C, K> Iterator for ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CachingStrategy<Item = U>,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
    K: Iterator<Item = String>,
{
    type Item = QueryResult<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.next()?;
        match self.caching_strategy.get_item(&key) {
            Some(cached_val) => {
                println!("Cache hit for key: {}", key);
                Some(Ok(cached_val))
            }
            None => {
                println!("Cache miss for key: {}, reading from inner", key);
                match self.inner.next() {
                    Some(Ok(val)) => {
                        self.caching_strategy.put_item(&key, &val);
                        Some(Ok(val))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
        }
    }
}

pub struct SelectCachingWrapper<T, C, U>
where
    U: Serialize,
    C: CachingStrategy<Item = U>,
{
    inner_select: T,
    caching: C,
}

impl<T, C, U> SelectCachingWrapper<T, C, U>
where
    U: Serialize,
    C: CachingStrategy<Item = U>,
{
    fn new(inner_select: T, caching: C) -> Self {
        Self {
            inner_select,
            caching,
        }
    }
}

impl<T, Conn, C, U> ExecuteDsl<Conn, Conn::Backend> for SelectCachingWrapper<T, C, U>
where
    T: ExecuteDsl<Conn>,
    Conn: Connection,
    U: Serialize,
    C: CachingStrategy<Item = U>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, U> RunQueryDsl<Conn> for SelectCachingWrapper<T, C, U>
where
    C: CachingStrategy<Item = U>,
    U: Serialize,
{
}

impl<'query, T, Conn, U, B, C> LoadQuery<'query, Conn, U, B> for SelectCachingWrapper<T, C, U>
where
    T: LoadQuery<'query, Conn, (U, String), B>,
    Conn: 'query,
    U: Serialize + std::fmt::Debug,
    C: CachingStrategy<Item = U>,
{
    type RowIter<'a>
        = ResultCachingIterator<T::RowIter<'a>, U, C>
    where
        Conn: 'a;

    fn internal_load(self, conn: &mut Conn) -> QueryResult<Self::RowIter<'_>> {
        println!("In internal_load (2)");

        let load_iter = self.inner_select.internal_load(conn)?;
        let caching_iter = ResultCachingIterator {
            inner: load_iter,
            caching_strategy: self.caching,
        };
        Ok(caching_iter)
    }
}

pub struct SelectCacheReadWrapper<T, C, U, K>
where
    U: Serialize,
    C: CachingStrategy<Item = U>,
    K: Iterator<Item = String>,
{
    inner_select: T,
    keys: K,
    caching: C,
}

impl<T, C, U, K> SelectCacheReadWrapper<T, C, U, K>
where
    U: Serialize,
    C: CachingStrategy<Item = U>,
    K: Iterator<Item = String>,
{
    fn new(inner_select: T, keys: K, caching: C) -> Self {
        Self {
            inner_select,
            keys,
            caching,
        }
    }
}

impl<T, Conn, C, U, K> ExecuteDsl<Conn, Conn::Backend> for SelectCacheReadWrapper<T, C, U, K>
where
    T: ExecuteDsl<Conn>,
    Conn: Connection,
    U: Serialize,
    C: CachingStrategy<Item = U>,
    K: Iterator<Item = String>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, U, K> RunQueryDsl<Conn> for SelectCacheReadWrapper<T, C, U, K>
where
    C: CachingStrategy<Item = U>,
    U: Serialize,
    K: Iterator<Item = String>,
{
}

impl<'query, T, Conn, U, B, C, K> LoadQuery<'query, Conn, U, B>
    for SelectCacheReadWrapper<T, C, U, K>
where
    T: LoadQuery<'query, Conn, U, B>,
    Conn: 'query,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
    C: CachingStrategy<Item = U>,
    K: Iterator<Item = String>,
{
    type RowIter<'a>
        = ResultCacheLookupIterator<T::RowIter<'a>, U, C, K>
    where
        Conn: 'a;

    fn internal_load(self, conn: &mut Conn) -> QueryResult<Self::RowIter<'_>> {
        println!("In internal_load (1)");

        let load_iter = self.inner_select.internal_load(conn)?;
        let lookup_iter = ResultCacheLookupIterator::new(load_iter, self.caching, self.keys);
        Ok(lookup_iter)
    }
}

pub trait WrappableQuery {
    fn cache_results<U>(
        self,
        cache: Rc<RefCell<StringCache>>,
    ) -> SelectCachingWrapper<Self, InMemoryCachingStrategy<U>, U>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCachingWrapper::new(self, InMemoryCachingStrategy::new(cache))
    }

    fn use_cache_key<'a, U>(
        self,
        cache: Rc<RefCell<StringCache>>,
        key: &'a str,
    ) -> SelectCacheReadWrapper<
        Self,
        InMemoryCachingStrategy<U>,
        U,
        <Vec<String> as IntoIterator>::IntoIter,
    >
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCacheReadWrapper::new(
            self,
            vec![key.to_string()].into_iter(),
            InMemoryCachingStrategy::new(cache),
        )
    }

    fn use_cache_keys<U, K>(
        self,
        cache: Rc<RefCell<StringCache>>,
        keys: K,
    ) -> SelectCacheReadWrapper<Self, InMemoryCachingStrategy<U>, U, K>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
        K: Iterator<Item = String>,
    {
        SelectCacheReadWrapper::new(self, keys, InMemoryCachingStrategy::new(cache))
    }
}

impl<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking> WrappableQuery
    for SelectStatement<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
{
}

