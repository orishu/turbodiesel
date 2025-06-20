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
use log::info;
use turbodiesel::redis_test_util::RedisTestUtil;
use turbodiesel::statement_wrappers::*;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    turbodiesel::test_utils::init_logging_for_tests();
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
            info!("Student: {:?}", student.unwrap());
        });

    info!("cache: {:?}", cache);

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
            info!("Student: {:?}", student.unwrap());
        });

    info!("Cache before update: {:?}", cache);
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori2"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating students");

    info!("Cache after update: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .use_cache_key::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            info!("Student: {:?}", student.unwrap());
        });
}

#[test]
#[cfg(feature = "redis")]
fn system_test_with_redis() {
    let redis_test = RedisTestUtil::new();
    redis_test.run_test_with_redis(async |redis_url, _| {
        inner_system_test_with_redis(redis_url).await;
    });
}

async fn inner_system_test_with_redis(redis_url: String) {
    use turbodiesel::{cacher::CacheHandle, redis_cacher::RedisCache};

    let cache = RedisCache::new(redis_url.as_str()).expect("Failed to create RedisCache");
    let handle = cache.handle();

    handle
        .wait_until_online(6)
        .await
        .expect("Redis is not online after retries");
    handle
        .load_redis_functions()
        .expect("Failed to load Redis functions");

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
            info!("Student: {:?}", student.unwrap());
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
            info!("Student: {:?}", student.unwrap());
        });

    info!(
        "Cache before update: {:?}",
        handle.scan_keys("student:*").unwrap()
    );
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori2"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating students");
    info!(
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
            info!("Student: {:?}", student.unwrap());
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
    info!("Serialized student: {}", serialized);
    let deserialized: Student = serde_json::from_str(&serialized).unwrap();
    assert_eq!(student, deserialized);
}
