use crate::cacher::CacheHandle;
use diesel::connection::Connection;
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub struct ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<(U, String)>>,
    C: CacheHandle,
    U: Serialize,
{
    inner: I,
    cache: C,
}

impl<I, U, C> Iterator for ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<(U, String)>>,
    C: CacheHandle,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
{
    type Item = QueryResult<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(ref it_res) = item {
            println!("Item result is {:?}", it_res);
            if let Ok(it) = it_res {
                self.cache.put::<U>(&it.1, &it.0);
                println!("Item cached (2)");
            }
        }
        item.map(|r| r.map(|pair| pair.0))
    }
}

pub struct ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CacheHandle,
    U: Serialize + DeserializeOwned,
    K: Iterator<Item = String>,
{
    inner: I,
    keys: K,
    cache: C,
}

impl<I, U, C, K> ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CacheHandle,
    U: Serialize + DeserializeOwned,
    K: Iterator<Item = String>,
{
    fn new(inner: I, cache: C, keys: K) -> Self {
        Self { inner, keys, cache }
    }
}

impl<I, U, C, K> Iterator for ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CacheHandle,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
    K: Iterator<Item = String>,
{
    type Item = QueryResult<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.next()?;
        match self.cache.get::<U>(&key) {
            Some(cached_val) => {
                println!("Cache hit for key: {}", key);
                Some(Ok(cached_val))
            }
            None => {
                println!("Cache miss for key: {}, reading from inner", key);
                match self.inner.next() {
                    Some(Ok(val)) => {
                        self.cache.put::<U>(&key, &val);
                        Some(Ok(val))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
        }
    }
}

pub struct SelectCachingWrapper<T, C>
where
    C: CacheHandle,
{
    inner_select: T,
    cache: C,
}

impl<T, C> SelectCachingWrapper<T, C>
where
    C: CacheHandle,
{
    fn new(inner_select: T, cache: C) -> Self {
        Self {
            inner_select,
            cache,
        }
    }
}

impl<T, Conn, C> ExecuteDsl<Conn, Conn::Backend> for SelectCachingWrapper<T, C>
where
    T: ExecuteDsl<Conn>,
    Conn: Connection,
    C: CacheHandle,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C> RunQueryDsl<Conn> for SelectCachingWrapper<T, C> where C: CacheHandle {}

impl<'query, T, Conn, U, B, C> LoadQuery<'query, Conn, U, B> for SelectCachingWrapper<T, C>
where
    T: LoadQuery<'query, Conn, (U, String), B>,
    Conn: 'query,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
    C: CacheHandle,
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
            cache: self.cache,
        };
        Ok(caching_iter)
    }
}

pub struct SelectCacheReadWrapper<T, C, K>
where
    C: CacheHandle,
    K: Iterator<Item = String>,
{
    inner_select: T,
    keys: K,
    cache: C,
}

impl<T, C, K> SelectCacheReadWrapper<T, C, K>
where
    C: CacheHandle,
    K: Iterator<Item = String>,
{
    fn new(inner_select: T, keys: K, cache: C) -> Self {
        Self {
            inner_select,
            keys,
            cache,
        }
    }
}

impl<T, Conn, C, K> ExecuteDsl<Conn, Conn::Backend> for SelectCacheReadWrapper<T, C, K>
where
    T: ExecuteDsl<Conn>,
    Conn: Connection,
    C: CacheHandle,
    K: Iterator<Item = String>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, K> RunQueryDsl<Conn> for SelectCacheReadWrapper<T, C, K>
where
    C: CacheHandle,
    K: Iterator<Item = String>,
{
}

impl<'query, T, Conn, U, B, C, K> LoadQuery<'query, Conn, U, B> for SelectCacheReadWrapper<T, C, K>
where
    T: LoadQuery<'query, Conn, U, B>,
    Conn: 'query,
    U: Serialize + DeserializeOwned + std::fmt::Debug,
    C: CacheHandle,
    K: Iterator<Item = String>,
{
    type RowIter<'a>
        = ResultCacheLookupIterator<T::RowIter<'a>, U, C, K>
    where
        Conn: 'a;

    fn internal_load(self, conn: &mut Conn) -> QueryResult<Self::RowIter<'_>> {
        println!("In internal_load (1)");

        let load_iter = self.inner_select.internal_load(conn)?;
        let lookup_iter = ResultCacheLookupIterator::new(load_iter, self.cache, self.keys);
        Ok(lookup_iter)
    }
}

pub trait WrappableQuery {
    type Cache: CacheHandle;

    fn cache_results<U>(self, cache: Self::Cache) -> SelectCachingWrapper<Self, Self::Cache>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCachingWrapper::new(self, cache)
    }

    fn use_cache_key<'a, U>(
        self,
        cache: Self::Cache,
        key: &'a str,
    ) -> SelectCacheReadWrapper<Self, Self::Cache, <Vec<String> as IntoIterator>::IntoIter>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCacheReadWrapper::new(self, vec![key.to_string()].into_iter(), cache)
    }

    fn use_cache_keys<U, K>(
        self,
        cache: Self::Cache,
        keys: K,
    ) -> SelectCacheReadWrapper<Self, Self::Cache, K>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
        K: Iterator<Item = String>,
    {
        SelectCacheReadWrapper::new(self, keys, cache)
    }
}

pub struct UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: CacheHandle,
{
    inner_update: T,
    keys: K,
    cache: C,
}

impl<T, K, C> UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: CacheHandle,
{
    fn new(inner_update: T, keys: K, cache: C) -> Self {
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
    C: CacheHandle,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        query.keys.for_each(|key| {
            println!("Invalidating cache for key: {}", key);
            query.cache.clone().delete(&key);
        });
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_update, conn)
    }
}

impl<T, Conn, K, C> RunQueryDsl<Conn> for UpdateWrapper<T, K, C>
where
    K: Iterator<Item = String>,
    C: CacheHandle,
{
}

pub trait WrappableUpdate {
    type Cache: CacheHandle;

    fn invalidate_key<'a>(
        self,
        cache: Self::Cache,
        key: &'a str,
    ) -> UpdateWrapper<Self, <Vec<String> as IntoIterator>::IntoIter, Self::Cache>
    where
        Self: Sized,
    {
        UpdateWrapper::new(self, vec![key.to_string()].into_iter(), cache)
    }

    fn invalidate_keys<K>(self, cache: Self::Cache, keys: K) -> UpdateWrapper<Self, K, Self::Cache>
    where
        Self: Sized,
        K: Iterator<Item = String>,
    {
        UpdateWrapper::new(self, keys, cache)
    }
}
