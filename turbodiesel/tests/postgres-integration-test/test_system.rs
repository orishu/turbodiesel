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
use turbodiesel::statement_wrappers::*;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    turbodiesel::test_utils::init_logging_for_tests();
}

#[test]
fn simple_insert_using_diesel() {
    let connection = &mut establish_connection();
    diesel::delete(students::table)
        .execute(connection)
        .expect("Error deleting existing students");
    fill_students_table(connection);
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
        .populate_cache::<Student>(handle.clone())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            info!("Student: {:?}", student.unwrap());
        });

    info!("cache: {:?}", cache);

    students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq_any(vec![1, 2]))
        .try_from_cache_multi::<Student, _>(
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
        .try_from_cache::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .for_each(|student| {
            info!("Student: {:?}", student.unwrap());
        });
}

#[tokio::test]
#[cfg(feature = "redis")]
async fn system_test_with_postgres_and_redis() {
    use diesel_migrations::embed_migrations;
    use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
    use turbodiesel::postgres_test_util::PostgresTestUtil;
    use turbodiesel::redis_test_util::RedisTestUtil;

    pub const MIGRATIONS: EmbeddedMigrations =
        embed_migrations!("tests/postgres-integration-test/migrations");

    let postgres_test = PostgresTestUtil::new();
    postgres_test
        .run_test_with_postgres(async |postgres_url, _| {
            let connection =
                &mut PgConnection::establish(&postgres_url).expect("Failed to connect to postgres");
            connection
                .run_pending_migrations(MIGRATIONS)
                .expect("failed running migrations");

            let redis_test = RedisTestUtil::new();
            redis_test
                .run_test_with_redis(async |redis_url, _| {
                    inner_system_test_with_postgres_and_redis(postgres_url, redis_url).await;
                })
                .await;
        })
        .await;
}

#[cfg(feature = "redis")]
async fn inner_system_test_with_postgres_and_redis(postgres_url: String, redis_url: String) {
    use turbodiesel::{cacher::CacheHandle, redis_cacher::RedisCache};

    let connection =
        &mut PgConnection::establish(&postgres_url).expect("Failed to connect to postgres");

    let cache = RedisCache::new(redis_url.as_str()).expect("Failed to create RedisCache");
    let handle = cache.handle();

    // Insert 3 rows to the table.
    fill_students_table(connection);

    // Sanity test that select() works.
    let test_students = make_test_students();
    let mut query_result: Vec<Student> = students::dsl::students
        .select(Student::as_select())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading students")
        .map(|s| s.unwrap())
        .collect();
    assert_eq!(query_result, test_students);

    let records_in_cache = handle.scan_keys("student:*").unwrap().capacity();
    assert_eq!(records_in_cache, 0);

    // Populate the cache with all students.
    let row_with_cache_key = (Student::as_select(), sql::<Text>("'student:' || id"));
    query_result = students::dsl::students
        .select(row_with_cache_key.clone())
        .populate_cache::<Student>(handle.clone())
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|s| s.unwrap())
        .collect();
    assert_eq!(query_result, test_students);
    let records_in_cache = handle.scan_keys("student:*").unwrap().capacity();
    assert_eq!(records_in_cache, 3);

    let mut cached_student: Option<Student> = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(cached_student, Some(test_students[1].clone()));

    // Update and invalidate one record.
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori2"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating student");

    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(cached_student, None);

    // Update student 2's name without invalidating the cache. The record is not
    // in cache, so querying for it would yield the newly updated value.
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori1"))
        .filter(students::dsl::id.eq(2))
        .execute(connection)
        .expect("Error updating student");

    // Update student 3's name without invalidating the cache. The record is cached,
    // so querying for it would yield the cached result which isn't the updated value.
    diesel::update(students::table)
        .set(students::dsl::name.eq("Dan1"))
        .filter(students::dsl::id.eq(3))
        .execute(connection)
        .expect("Error updating student");

    // Select with trying the cache - student 3 will result in the stale cached record.
    query_result = students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(3))
        .try_from_cache::<Student>(handle.clone(), "student:3")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|student| student.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 3,
            name: "Dan".to_string(),
            dob: Some(date_from_string("2009-04-12")),
        }]
    );

    // Select with trying the cache with two keys student 1 and 3.
    // Student 3 will result in the stale cached record.
    query_result = students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(3))
        .try_from_cache_multi::<Student, _>(
            handle.clone(),
            vec!["student:1".to_string(), "student:3".to_string()].into_iter(),
        )
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|student| student.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![
            Student {
                id: 1,
                name: "John".to_string(),
                dob: None,
            },
            Student {
                id: 3,
                name: "Dan".to_string(),
                dob: Some(date_from_string("2009-04-12")),
            },
        ]
    );

    // Select with trying the cache - student 2 was invalidated from cache so we expect the
    // latest updated value from the database.
    query_result = students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .try_from_cache::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|student| student.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 2,
            name: "Ori1".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        }]
    );

    // Verify that student 2 was not yet populated in cache.
    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(cached_student, None);

    // Select with trying the cache AND populate - student 2 is not in the cache so we expect the
    // latest updated value from the database, and then, that value to be cached.
    query_result = students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .try_from_cache_and_populate::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|student| student.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 2,
            name: "Ori1".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        }]
    );

    // Verify that student 2 was populated in cache.
    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(
        cached_student,
        Some(Student {
            id: 2,
            name: "Ori1".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        })
    );

    // Update and invalidate student 2
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori3"))
        .filter(students::dsl::id.eq(2))
        .invalidate_key(handle.clone(), "student:2")
        .execute(connection)
        .expect("Error updating student");
    // Verify that student 2 was not yet populated in cache.
    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(cached_student, None);

    // Combine populate_cache and try_from_cache when the record is not yet in cache.
    query_result = students::dsl::students
        .select(row_with_cache_key.clone())
        .filter(students::dsl::id.eq(2))
        .populate_cache::<Student>(handle.clone())
        .try_from_cache::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|s| s.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 2,
            name: "Ori3".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        }]
    );
    // Verify that student 2 was populated in cache.
    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(
        cached_student,
        Some(Student {
            id: 2,
            name: "Ori3".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        })
    );

    // Update student 2's name without invalidating the cache. The record is cached,
    // so querying for it would yield the cached result which isn't the updated value.
    diesel::update(students::table)
        .set(students::dsl::name.eq("Ori4"))
        .filter(students::dsl::id.eq(2))
        .execute(connection)
        .expect("Error updating student");

    // Combine populate_cache and try_from_cache when the record is in cache (and stale,
    // because we just updated it without invalidating).
    query_result = students::dsl::students
        .select(row_with_cache_key.clone())
        .filter(students::dsl::id.eq(2))
        .populate_cache::<Student>(handle.clone())
        .try_from_cache::<Student>(handle.clone(), "student:2")
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|s| s.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 2,
            name: "Ori3".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        }]
    );
    // Verify that student 2 has the same stale value in cache.
    cached_student = cache.handle().get(&"student:2".to_string()).unwrap();
    assert_eq!(
        cached_student,
        Some(Student {
            id: 2,
            name: "Ori3".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        })
    );

    // Read student 2 directly from the database to verify it has the latest updated value.
    query_result = students::dsl::students
        .select(Student::as_select())
        .filter(students::dsl::id.eq(2))
        .load_iter::<Student, DefaultLoadingMode>(connection)
        .expect("Error loading student")
        .map(|s| s.unwrap())
        .collect();
    assert_eq!(
        query_result,
        vec![Student {
            id: 2,
            name: "Ori4".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        }]
    );
}

#[test]
fn test_basic_json_serialization() {
    let student = Student {
        id: 1,
        name: "John".to_string(),
        dob: Some(date_from_string("1978-02-14")),
    };
    let serialized = serde_json::to_string(&student).unwrap();
    info!("Serialized student: {}", serialized);
    let deserialized: Student = serde_json::from_str(&serialized).unwrap();
    assert_eq!(student, deserialized);
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

fn fill_students_table(connection: &mut PgConnection) {
    let records = make_test_students();
    diesel::insert_into(students::table)
        .values(records)
        .returning(Student::as_returning())
        .get_results::<Student>(connection)
        .expect("Error saving new student");
}

fn make_test_students() -> Vec<Student> {
    vec![
        Student {
            id: 1,
            name: "John".to_string(),
            dob: None,
        },
        Student {
            id: 2,
            name: "Ori".to_string(),
            dob: Some(date_from_string("1978-02-14")),
        },
        Student {
            id: 3,
            name: "Dan".to_string(),
            dob: Some(date_from_string("2009-04-12")),
        },
    ]
}
