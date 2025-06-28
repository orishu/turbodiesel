use diesel::pg;
use diesel::prelude::*;
use serde::{Deserialize, Serialize, ser::SerializeTuple};
use std::option::Option;

impl Serialize for Student {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut tup = serializer.serialize_tuple(3)?;
        tup.serialize_element(&self.id)?;
        tup.serialize_element(&self.name.as_str())?;
        tup.serialize_element(&self.dob.map(|dob| dob.0))?;
        tup.end()
    }
}

impl<'de> Deserialize<'de> for Student {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (id, name, dob) = <(i32, String, Option<i32>)>::deserialize(deserializer)?;
        Ok(Student {
            id,
            name,
            dob: dob.map(|d| pg::data_types::PgDate(d)),
        })
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, PartialEq, Clone)]
#[diesel(table_name = crate::schema::students)]
#[diesel(check_for_backend(pg::Pg))]
pub struct Student {
    pub id: i32,
    pub name: String,
    pub dob: Option<pg::data_types::PgDate>,
}
