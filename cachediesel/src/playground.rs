use diesel::{Queryable, Selectable};
use postgres::NoTls;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Client, Error};

use diesel::pg::data_types::PgDate;
use diesel::prelude::*;
use dotenvy::dotenv;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::iter::Map;
use std::marker::PhantomData;

use diesel::backend::Backend;
use diesel::connection::{Connection, DefaultLoadingMode, LoadConnection};
use diesel::helper_types::Limit;
use diesel::query_builder::{AsQuery, SelectStatement};
use diesel::query_dsl::load_dsl::ExecuteDsl;
use diesel::query_dsl::{LoadQuery, RunQueryDsl, methods};
use diesel::result::QueryResult;
use julian::{Calendar, Month, system2jdn};
use std::iter::Inspect;

struct Cache<K: Eq + Hash, V> {
    map: HashMap<K, V>,
}

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

struct SelectWrapper<'conn, Conn, T, U, B = DefaultLoadingMode>
where
    T: for<'query> LoadQuery<'query, Conn, U, B>,
    Conn: LoadConnection<B> + 'conn,
    U: std::fmt::Debug + 'conn,
 {
    inner_select: T,
    phantom_u: PhantomData<&'conn U>,
    phantom_b: PhantomData<B>,
    phantom_c: PhantomData<&'conn Conn>,
}

impl<'conn, Conn, T, U, B> SelectWrapper<'conn, Conn, T, U, B>
where
    T: for<'query> LoadQuery<'query, Conn, U, B>,
    Conn: LoadConnection<B> + 'conn,
    U: std::fmt::Debug + 'conn,
 {
    fn new(inner_select: T) -> Self {
        Self {
            inner_select,
            phantom_u: PhantomData,
            phantom_b: PhantomData,
            phantom_c: PhantomData,
        }
    }
}

impl<'conn, Conn, T: ExecuteDsl<Conn>, U, B> ExecuteDsl<Conn, Conn::Backend>
    for SelectWrapper<'conn, Conn, T, U, B>
where
    T: ExecuteDsl<Conn> + for<'query> LoadQuery<'query, Conn, U, B>,
    Conn: LoadConnection<B> + 'conn,
    U: std::fmt::Debug + 'conn,
{
    fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
        ExecuteDsl::<Conn, Conn::Backend>::execute(query.inner_select, conn)
    }
}

impl<'conn, Conn, T, U, B> RunQueryDsl<Conn> for SelectWrapper<'conn, Conn, T, U, B>
where
//    T: for<'query> LoadQuery<'query, Conn, U, B>,
    T: for<'query> LoadQuery<'query, Conn, U, B> + RunQueryDsl<Conn>, // <-- Add this
    Conn: LoadConnection<B> + 'conn,
    U: std::fmt::Debug + 'conn,
 {}

impl<'query, 'conn, T, Conn, U, B> LoadQuery<'query, Conn, U, B> for SelectWrapper<'conn, Conn, T, U, B>
where
    T: LoadQuery<'query, Conn, U, B> + RunQueryDsl<Conn>,
    Conn: LoadConnection<B> + 'conn,
    U: std::fmt::Debug + 'conn,
{
    type RowIter<'a> = Box<dyn Iterator<Item = QueryResult<U>> + 'a>
    where
        Conn: 'a;

    /*
    type RowIter<'a>
        = T::RowIter<'a>
    where
        Conn: 'a;
        */

    fn internal_load(self, conn: &mut Conn) -> QueryResult<Self::RowIter<'_>>
        where U: 'conn
     {
        println!("In internal_load");
        //self.inner_select.internal_load(conn)

        let results: Vec<QueryResult<U>> = self.inner_select.internal_load(conn)?.collect();
        Ok(Box::new(results.into_iter()))
//        Ok(results)



        /*/
        let result = self.inner_select.internal_load(conn);
        if let Ok(ref res) = result {
            let cloned = res.by_ref().cloned();
            cloned.for_each(|f| println!("Peeking: {:?}", f));
        }
        result
        */
    }
}

trait WrappableQuery<B = DefaultLoadingMode> {
    fn wrap_query<U>(self) -> SelectWrapper<Self, U, B>
    where
        Self: Sized,
    {
        SelectWrapper::<Self, U, B>::new(self)
    }
}

impl<B, From, Select, Distinct, Where, Order, LimitOffset, GroupBy, Having, Locking>
    WrappableQuery<B>
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
        let connection = &mut establish_connection();
        let select_statement = 
            schema::students::dsl::students
                .select(Student::as_select())
                .filter(schema::students::dsl::id.gt(1));
        let it = select_statement
            .wrap_query::<Student>()
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
}
