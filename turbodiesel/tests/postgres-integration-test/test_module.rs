use crate::models::Student;
use crate::pgutils::establish_connection;
use crate::schema::students;
use chrono::Utc;
use diesel::connection::DefaultLoadingMode;
use diesel::dsl::sql;
use diesel::pg::data_types::PgDate;
use diesel::prelude::*;
use diesel::sql_types::Text;
use julian::{Calendar, Month, system2jdn};
use lazy_static::lazy_static;
use postgres::Client;
use postgres::NoTls;
use postgres::fallible_iterator::FallibleIterator;
use std::cell::RefCell;
use std::rc::Rc;
use turbodiesel::cacher::*;
use turbodiesel::statement_wrappers::*;

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
fn simple_update_using_diesel() {
    let connection = &mut establish_connection();
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori1"))
        .filter(students::dsl::id.eq(2))
        .execute(connection)
        .expect("Error updating students");
}

#[test]
fn simple_select_using_diesel() {
    let cache = Rc::new(RefCell::new(StringCache::new()));
    //        let student_row = (students::dsl::id, students::dsl::name, students::dsl::dob);
    let row_with_cache_key = (Student::as_select(), sql::<Text>("'student:' || id"));

    let connection = &mut establish_connection();

    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori1"))
        .filter(students::dsl::id.eq(2))
        .execute(connection)
        .expect("Error updating students");

    students::dsl::students
        .select(row_with_cache_key)
        .filter(students::dsl::id.eq(2))
        .cache_results(cache.clone())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });

    println!("cache: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq_any(vec![1, 2]))
        .use_cache_keys(
            cache.clone(),
            vec!["student:1".to_string(), "student:2".to_string()].into_iter(),
        )
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });

    println!("Cache before update: {:?}", cache);
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori2"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(cache.clone(), "student:2")
        .execute(connection)
        .expect("Error updating students");

    println!("Cache after update: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .use_cache_key(cache.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });
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
