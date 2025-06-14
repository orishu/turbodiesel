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
use turbodiesel::statement_wrappers::*;

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
    diesel::delete(students::table)
        .execute(connection)
        .expect("Error deleting existing students");
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
#[cfg(feature = "inmemory")]
fn system_test_with_inmemory_cache() {
    use turbodiesel::cacher::HashmapCache;

    let cache = HashmapCache::new();
    let handle = cache.handle();
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
        .cache_results::<Student>(handle.clone())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });

    println!("cache: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq_any(vec![1, 2]))
        .use_cache_keys::<Student, _>(
            handle.clone(),
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
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating students");

    println!("Cache after update: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .use_cache_key::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });
}

#[test]
#[cfg(feature = "redis")]
fn system_test_with_redis() {
    use turbodiesel::redis_cacher::RedisCache;

    let cache = RedisCache::new("redis://localhost:6379").expect("Failed to create RedisCache");
    let handle = cache.handle();
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
        .cache_results::<Student>(handle.clone())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq_any(vec![1, 2]))
        .use_cache_keys::<Student, _>(
            handle.clone(),
            vec!["student:1".to_string(), "student:2".to_string()].into_iter(),
        )
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            println!("Student: {:?}", student.unwrap());
        });

    println!(
        "Cache before update: {:?}",
        handle.scan_keys("student:*").unwrap()
    );
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori2"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating students");
    println!(
        "Cache after update: {:?}",
        handle.scan_keys("student:*").unwrap()
    );

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .use_cache_key::<Student>(handle.clone(), "student:2")
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
