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

use crate::schema::students;
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
    type Item: Serialize;

    fn put_in_cache(&self, key: String, value: String);

    fn put_item(&self, key: &String, item: &Self::Item) {
        self.put_in_cache(key.clone(), serde_json::to_string(item).unwrap());
    }
}

struct InMemoryCachingStrategy<U>
where
    U: Serialize,
{
    cache: Rc<RefCell<StringCache>>,
    phantom_data: PhantomData<U>,
}

impl<U> InMemoryCachingStrategy<U>
where
    U: Serialize,
{
    fn new(cache: Rc<RefCell<StringCache>>) -> Self {
        Self {
            cache,
            phantom_data: PhantomData,
        }
    }
}

impl<U> CachingStrategy for InMemoryCachingStrategy<U>
where
    U: Serialize,
{
    type Item = U;

    fn put_in_cache(&self, key: String, value: String) {
        let mut c = self.cache.borrow_mut();
        c.put(key, value);
    }
}

struct ResultCachingIterator<I, U, C>
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

struct SelectWrapper<T, C, U>
where
    U: Serialize,
    C: CachingStrategy<Item = U>,
{
    inner_select: T,
    caching: C,
}

impl<T, C, U> SelectWrapper<T, C, U>
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

impl<T: ExecuteDsl<Conn>, Conn, C, U> ExecuteDsl<Conn, Conn::Backend> for SelectWrapper<T, C, U>
where
    Conn: Connection,
    U: Serialize,
    C: CachingStrategy<Item = U>,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<T, Conn, C, U> RunQueryDsl<Conn> for SelectWrapper<T, C, U>
where
    C: CachingStrategy<Item = U>,
    U: Serialize,
{
}

impl<'query, T, Conn, U, B, C> LoadQuery<'query, Conn, U, B> for SelectWrapper<T, C, U>
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

trait WrappableQuery {
    fn cache_results<U>(
        self,
        cache: Rc<RefCell<StringCache>>,
    ) -> SelectWrapper<Self, InMemoryCachingStrategy<U>, U>
    where
        Self: Sized,
        U: Serialize,
    {
        SelectWrapper::new(self, InMemoryCachingStrategy::new(cache))
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
    //use diesel::query_dsl::methods::SelectDsl;
    use diesel::RunQueryDsl;
    use diesel::expression::AsExpression;
    use diesel::pg::Pg;
    use diesel::query_builder::QueryFragment;

    use crate::establish_connection;
    use crate::models::Student;
    use crate::schema;
    use crate::schema::students::{self, dob};
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
        //        let student_row = (students::dsl::id, students::dsl::name, students::dsl::dob);
        let row_with_cache_key = (Student::as_select(), sql::<Text>("'student:' || id"));

        let connection = &mut establish_connection();
        students::dsl::students
            .select(row_with_cache_key)
            .filter(schema::students::dsl::id.eq(2))
            .cache_results(new_cache.clone())
            .load_iter::<Student, DefaultLoadingMode>(connection)
            .expect("Error loading student")
            .for_each(|student| {
                println!("Student: {:?}", student.unwrap());
            });

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
