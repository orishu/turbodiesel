use diesel::{Queryable, Selectable};
use postgres::NoTls;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, Error};

use diesel::pg::data_types::PgDate;
use diesel::prelude::*;
use dotenvy::dotenv;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::iter::{Inspect, Map};
use std::marker::PhantomData;
use std::rc::Rc;

use diesel::backend::Backend;
use diesel::connection::{Connection, DefaultLoadingMode, LoadConnection};
use diesel::helper_types::Limit;
use diesel::query_builder::{AsQuery, SelectStatement};
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl, methods};
use diesel::result::QueryResult;
use julian::{Calendar, Month, system2jdn};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct Cache<K: Eq + Hash, V> {
    map: HashMap<K, V>,
}

type StringCache = Cache<String, String>;

trait Cacher {
    type Key: Eq + Hash;
    type Value;

    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
    fn put(&mut self, key: Self::Key, value: Self::Value);
}

impl<K: Eq + Hash, V> Cache<K, V> {
    fn new() -> Cache<K, V> {
        Cache {
            map: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash, V> Cacher for Cache<K, V> {
    type Key = K;
    type Value = V;

    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }
    fn put(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }
}

struct CachingIterator<'a, R, I: Iterator<Item = R>, K: Eq + Hash, V, C> {
    inner_iter: I,

    key_value_transform: fn(&R) -> (K, V),
    cacher: &'a mut C,
}

impl<'a, R, I: Iterator<Item = R>, K: Eq + Hash, V, C: Cacher<Key = K, Value = V>>
    CachingIterator<'a, R, I, K, V, C>
{
    fn new(
        cacher: &'a mut C,
        inner_iter: I,
        key_value_transform: fn(&R) -> (K, V),
    ) -> CachingIterator<'a, R, I, K, V, C> {
        CachingIterator {
            inner_iter,
            key_value_transform,
            cacher,
        }
    }
}

impl<'a, R, I: Iterator<Item = R>, K: Eq + Hash, V, C: Cacher<Key = K, Value = V>> Iterator
    for CachingIterator<'a, R, I, K, V, C>
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner_iter.next();
        if let Some(ref rec) = item {
            let (key, value) = (self.key_value_transform)(rec);
            self.cacher.put(key, value);
        }
        item
    }
}

trait CachingStrategy {
    type Item;

    fn gen_key_value(&self, item: &Self::Item) -> (String, String);

    fn put_in_cache(&self, key: String, value: String);
}

struct InMemoryCachingStrategy<U> {
    cache: Rc<RefCell<StringCache>>,
    key_value_func: fn(&U) -> (String, String),
}

impl<U> InMemoryCachingStrategy<U> {
    fn new(cache: Rc<RefCell<StringCache>>, key_value_func: fn(&U) -> (String, String)) -> Self {
        Self {
            cache,
            key_value_func,
        }
    }
}

impl<U> CachingStrategy for InMemoryCachingStrategy<U> {
    type Item = U;
    fn gen_key_value(&self, item: &Self::Item) -> (String, String) {
        (self.key_value_func)(item)
    }
    fn put_in_cache(&self, key: String, value: String) {
        let mut c = self.cache.borrow_mut();
        c.put(key, value);
    }
}

struct SelectWrapper<T, C, U>
where
    C: for<'a> CachingStrategy<Item = U>,
{
    inner_select: T,
    caching: C,
}

impl<'a, T, C, U> SelectWrapper<T, C, U>
where
    C: CachingStrategy<Item = U>,
{
    fn new(inner_select: T, caching: C) -> Self {
        Self {
            inner_select,
            caching,
        }
    }
}

impl<T: ExecuteDsl<Conn>, Conn, C, U> ExecuteDsl<Conn, Conn::Backend> for SelectWrapper<T, C, U>
where
    Conn: Connection,
    C: CachingStrategy<Item = U>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, U> RunQueryDsl<Conn> for SelectWrapper<T, C, U> where C: CachingStrategy<Item = U> {}

struct ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CachingStrategy<Item = U>,
{
    inner: I,
    caching_strategy: C,
}

impl<I, U, C> Iterator for ResultCachingIterator<I, U, C>
where
    I: Iterator<Item = QueryResult<U>>,
    C: CachingStrategy<Item = U>,
    U: std::fmt::Debug,
{
    type Item = QueryResult<U>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(Ok(ref it)) = item {
            println!("Item is {:?}", it);
            let (key, value) = self.caching_strategy.gen_key_value(it);
            self.caching_strategy.put_in_cache(key, value);
        }
        item
    }
}

impl<'query, T, Conn, U, B, C> LoadQuery<'query, Conn, U, B> for SelectWrapper<T, C, U>
where
    T: LoadQuery<'query, Conn, U, B>,
    Conn: 'query,
    U: std::fmt::Debug,
    C: CachingStrategy<Item = U>,
{
    type RowIter<'a>
        = ResultCachingIterator<T::RowIter<'a>, U, C>
    where
        Conn: 'a;

    fn internal_load(self, conn: &mut Conn) -> QueryResult<Self::RowIter<'_>> {
        println!("In internal_load");
        let load_iter = self.inner_select.internal_load(conn)?;
        let caching_iter = ResultCachingIterator {
            inner: load_iter,
            caching_strategy: self.caching,
        };
        Ok(caching_iter)
    }
}

trait WrappableQuery {
    fn wrap_query<'a, C, U>(self, caching: C) -> SelectWrapper<Self, C, U>
    where
        Self: Sized,
        C: CachingStrategy<Item = U> + 'a,
    {
        SelectWrapper::<Self, C, U>::new(self, caching)
    }
}

impl<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking> WrappableQuery
    for SelectStatement<From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
{
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use chrono::Utc;
    use diesel::RunQueryDsl;

    use crate::establish_connection;
    use crate::models::Student;
    use crate::schema;
    use crate::schema::students;
    use lazy_static::lazy_static;

    use super::*;
    #[test]
    fn raw_query() {
        let mut client = Client::connect("host=localhost user=ori dbname=qflow", NoTls).unwrap();

        let results = client
            .query_raw("SELECT * FROM students", Vec::<&str>::new())
            .unwrap()
            .iterator()
            .map(Result::unwrap);

        results.for_each(|row| {
            println!("{:?}", row);
        });

        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn simple_insert_using_diesel() {
        let connection = &mut establish_connection();
        let records = vec![
            Student {
                id: 1,
                name: "John".to_string(),
                dob: None,
            },
            Student {
                id: 2,
                name: "Ori".to_string(),
                dob: Some(date_from_string("1978-02-16")),
            },
            Student {
                id: 3,
                name: "Dan".to_string(),
                dob: Some(date_from_string("2009-04-18")),
            },
        ];
        diesel::insert_into(students::table)
            .values(records)
            .returning(Student::as_returning())
            .get_results::<Student>(connection)
            .expect("Error saving new student");
    }

    #[test]
    fn simple_select_using_diesel() {
        let new_cache = Rc::new(RefCell::new(StringCache::new()));
        let caching_strategy = InMemoryCachingStrategy::new(new_cache.clone(), |s: &Student| {
            (s.id.to_string(), s.name.clone())
        });

        let connection = &mut establish_connection();
        let select_statement = schema::students::dsl::students
            .select(Student::as_select())
            .filter(schema::students::dsl::id.gt(1));
        let it = select_statement
            .wrap_query(caching_strategy)
            .load::<Student>(connection)
            .expect("load failed")
            .into_iter();

        //            .load_iter::<Student, DefaultLoadingMode>(connection)
        //            .expect("load failed")
        //            .map(Result::unwrap);

        let mut cache = Cache::<i32, String>::new();
        let caching_it = CachingIterator::new(&mut cache, it, |r| (r.id, r.name.clone()));
        caching_it.for_each(|x| println!("{:?}", x));

        println!("Cached student 1: {:?}", cache.get(&1));
        println!("Cached student 2: {:?}", cache.get(&2));

        println!("New cache: {:?}", new_cache);
    }

    lazy_static! {
        static ref JULIAN_DAY_2000: i32 = Calendar::GREGORIAN
            .at_ymd(2000, Month::January, 1)
            .unwrap()
            .julian_day_number();
    }

    fn date_from_string(date_str: &str) -> PgDate {
        let parsed_date = dateparser::parse_with_timezone(date_str, &Utc).unwrap();
        PgDate(system2jdn(parsed_date.into()).unwrap().0 - *JULIAN_DAY_2000)
    }

    #[test]
    fn test_basic_json_serialization() {
        let student = Student {
            id: 1,
            name: "John".to_string(),
            dob: Some(date_from_string("1978-02-16")),
        };
        let serialized = serde_json::to_string(&student).unwrap();
        println!("Serialized student: {}", serialized);
        let deserialized: Student = serde_json::from_str(&serialized).unwrap();
        assert_eq!(student, deserialized);
    }
}
