use diesel::prelude::*;
use std::option::Option;
use diesel::pg;

#[derive(Queryable, Selectable, Insertable, Debug)]
#[diesel(table_name = crate::schema::students)]
#[diesel(check_for_backend(pg::Pg))]
pub struct Student {
    pub id: i32,
    pub name: String,
    pub dob: Option<pg::data_types::PgDate>,
}
