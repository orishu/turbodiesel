use crate::cacher::CacheHandle;
use diesel::connection::Connection;
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;
use log::{debug, error, warn};
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
            debug!("Item result is {:?}", it_res);
            if let Ok(it) = it_res {
                let res = self.cache.put::<U>(&it.1, &it.0);
                if let Err(e) = res {
                    warn!("Error caching value for key {}: {}", it.1, e);
                } else {
                    debug!("Item cached");
                }
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
    populate: bool,
}

impl<I, U, C, K> ResultCacheLookupIterator<I, U, C, K>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CacheHandle,
    U: Serialize + DeserializeOwned,
    K: Iterator<Item = String>,
{
    fn new(inner: I, cache: C, keys: K, populate: bool) -> Self {
        Self {
            inner,
            keys,
            cache,
            populate,
        }
    }

    fn call_inner_and_cache(&mut self, key: &String) -> Option<QueryResult<U>> {
        match self.inner.next() {
            Some(Ok(val)) => {
                if self.populate {
                    let res = self.cache.put::<U>(key, &val);
                    if let Err(e) = res {
                        warn!("Error caching value for key {}: {}", key, e);
                    }
                }
                Some(Ok(val))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
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
            Ok(Some(cached_val)) => {
                debug!("Cache hit for key: {}", key);
                Some(Ok(cached_val))
            }
            Ok(None) => {
                debug!("Cache miss for key: {}, reading from inner", key);
                self.call_inner_and_cache(&key)
            }
            Err(e) => {
                warn!("Error retrieving from cache for key: {}; error {}", key, e);
                self.call_inner_and_cache(&key);
                None
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
        debug!("In SelectCachingWrapper internal_load");

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
    populate: bool,
}

impl<T, C, K> SelectCacheReadWrapper<T, C, K>
where
    C: CacheHandle,
    K: Iterator<Item = String>,
{
    fn new(inner_select: T, keys: K, cache: C, populate: bool) -> Self {
        Self {
            inner_select,
            keys,
            cache,
            populate,
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
        debug!("In SelectCacheReadWrapper internal_load");

        let load_iter = self.inner_select.internal_load(conn)?;
        let lookup_iter =
            ResultCacheLookupIterator::new(load_iter, self.cache, self.keys, self.populate);
        Ok(lookup_iter)
    }
}

pub trait WrappableQuery {
    type Cache: CacheHandle;

    fn populate_cache<U>(self, cache: Self::Cache) -> SelectCachingWrapper<Self, Self::Cache>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCachingWrapper::new(self, cache)
    }

    fn try_from_cache<'a, U>(
        self,
        cache: Self::Cache,
        key: &'a str,
    ) -> SelectCacheReadWrapper<Self, Self::Cache, <Vec<String> as IntoIterator>::IntoIter>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCacheReadWrapper::new(self, vec![key.to_string()].into_iter(), cache, false)
    }

    fn try_from_cache_and_populate<'a, U>(
        self,
        cache: Self::Cache,
        key: &'a str,
    ) -> SelectCacheReadWrapper<Self, Self::Cache, <Vec<String> as IntoIterator>::IntoIter>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
    {
        SelectCacheReadWrapper::new(self, vec![key.to_string()].into_iter(), cache, true)
    }

    fn try_from_cache_multi<U, K>(
        self,
        cache: Self::Cache,
        keys: K,
    ) -> SelectCacheReadWrapper<Self, Self::Cache, K>
    where
        Self: Sized,
        U: Serialize + DeserializeOwned,
        K: Iterator<Item = String>,
    {
        SelectCacheReadWrapper::new(self, keys, cache, false)
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
        for key in query.keys {
            debug!("Invalidating cache for key: {}", key);
            if let Err(e) = query.cache.clone().delete(&key) {
                error!("Error deleting key {} from cache: {}", key, e);
                return Err(diesel::result::Error::RollbackTransaction);
            }
        }
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
