use crate::cacher::Cacher;
use crate::cacher::CachingStrategy;
use crate::cacher::InMemoryCachingStrategy;
use diesel::connection::Connection;
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::rc::Rc;

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
    U: Serialize + DeserializeOwned,
    C: CachingStrategy<Item = U>,
{
    inner_select: T,
    caching: C,
}

impl<T, C, U> SelectCachingWrapper<T, C, U>
where
    U: Serialize + DeserializeOwned,
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
    U: Serialize + DeserializeOwned,
    C: CachingStrategy<Item = U>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, U> RunQueryDsl<Conn> for SelectCachingWrapper<T, C, U>
where
    C: CachingStrategy<Item = U>,
    U: Serialize + DeserializeOwned,
{
}

impl<'query, T, Conn, U, B, C> LoadQuery<'query, Conn, U, B> for SelectCachingWrapper<T, C, U>
where
    T: LoadQuery<'query, Conn, (U, String), B>,
    Conn: 'query,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
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
    U: Serialize + DeserializeOwned,
    C: CachingStrategy<Item = U>,
    K: Iterator<Item = String>,
{
    inner_select: T,
    keys: K,
    caching: C,
}

impl<T, C, U, K> SelectCacheReadWrapper<T, C, U, K>
where
    U: Serialize + DeserializeOwned,
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
    U: Serialize + DeserializeOwned,
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
    U: Serialize + DeserializeOwned,
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
    type Cache: Cacher<Key = String, Value = String>;

    fn cache_results<U>(
        self,
        cache: Rc<RefCell<Self::Cache>>,
    ) -> SelectCachingWrapper<Self, InMemoryCachingStrategy<Self::Cache, U>, U>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCachingWrapper::new(self, InMemoryCachingStrategy::new(cache))
    }

    fn use_cache_key<'a, U>(
        self,
        cache: Rc<RefCell<Self::Cache>>,
        key: &'a str,
    ) -> SelectCacheReadWrapper<
        Self,
        InMemoryCachingStrategy<Self::Cache, U>,
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
        cache: Rc<RefCell<Self::Cache>>,
        keys: K,
    ) -> SelectCacheReadWrapper<Self, InMemoryCachingStrategy<Self::Cache, U>, U, K>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
        K: Iterator<Item = String>,
    {
        SelectCacheReadWrapper::new(self, keys, InMemoryCachingStrategy::new(cache))
    }
}

pub struct UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: Cacher<Key = String, Value = String>,
{
    inner_update: T,
    keys: K,
    cache: Rc<RefCell<C>>,
}

impl<T, K, C> UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: Cacher<Key = String, Value = String>,
{
    fn new(inner_update: T, keys: K, cache: Rc<RefCell<C>>) -> Self {
        Self {
            inner_update,
            keys,
            cache,
        }
    }
}

impl<T, Conn, K, C> ExecuteDsl<Conn, Conn::Backend> for UpdateWrapper<T, K, C>
where
    T: ExecuteDsl<Conn>,
    Conn: Connection,
    K: Iterator<Item = String>,
    C: Cacher<Key = String, Value = String>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        let mut cache = query.cache.borrow_mut();
        query.keys.for_each(|key| {
            println!("Invalidating cache for key: {}", key);
            cache.delete(key);
        });
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_update, conn)
    }
}

impl<T, Conn, K, C> RunQueryDsl<Conn> for UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: Cacher<Key = String, Value = String>,
{
}

pub trait WrappableUpdate {
    type Cache: Cacher<Key = String, Value = String>;

    fn invalidate_key<'a>(
        self,
        cache: Rc<RefCell<Self::Cache>>,
        key: &'a str,
    ) -> UpdateWrapper<Self, <Vec<String> as IntoIterator>::IntoIter, Self::Cache>
    where
        Self: Sized,
    {
        UpdateWrapper::new(self, vec![key.to_string()].into_iter(), cache)
    }

    fn invalidate_keys<K>(
        self,
        cache: Rc<RefCell<Self::Cache>>,
        keys: K,
    ) -> UpdateWrapper<Self, K, Self::Cache>
    where
        Self: Sized,
        K: Iterator<Item = String>,
    {
        UpdateWrapper::new(self, keys, cache)
    }
}
