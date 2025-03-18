use core::fmt;

use partially::Partial;
use postgres::fallible_iterator::FallibleIterator;

use postgres::{Client, NoTls};

use partially_derive::Partial;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

struct ComputeState {
    client: Client,
}

trait Flowable {
    type Type;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = Self::Type>;
}

#[derive(Debug, Partial, Default, Serialize)]
#[partially(derive(Debug, Default))]
struct TestTableRow {
    id: i64,
    name: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct OtherTestTableRow {
    id: i64,

    #[serde(rename = "the_name")]
    name: String,
}

impl From<postgres::Row> for TestTableRow {
    fn from(row: postgres::Row) -> Self {
        TestTableRow {
            id: i32::into(row.get("id")),
            name: row.get("name"),
        }
    }
}

#[derive(Debug)]
struct TestTable {}

impl Flowable for TestTable {
    type Type = TestTableRow;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = TestTableRow> {
        vec![
            TestTableRow {
                id: 1,
                name: "Alice".to_string(),
            },
            TestTableRow {
                id: 2,
                name: "Bob".to_string(),
            },
        ]
        .into_iter()
    }
}

#[derive(Debug)]
struct Project<I, F: Flowable<Type = I>, O> {
    input: F,
    projection: fn(I) -> O,
}

impl<I, F: Flowable<Type = I>, O> Flowable for Project<I, F, O> {
    type Type = O;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = O> {
        self.input.compute(state).map(self.projection)
    }
}

#[derive(Debug)]
struct Select<I: Partial<Item = O>, F: Flowable<Type = I>, O> {
    input: F,
    selecting: fn(I) -> O,
}

impl<I: Partial<Item = O>, F: Flowable<Type = I>, O> Flowable for Select<I, F, O> {
    type Type = O;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = O> {
        self.input.compute(state).map(self.selecting)
    }
}

#[derive(Debug)]
struct TableData<'a, R> {
    table_name: &'a str,
    _marker: std::marker::PhantomData<R>,
    field_names: Vec<String>,
}

impl<'a, R: Default + Serialize> TableData<'a, R> {
    fn new(table_name: &'a str) -> Self {
        TableData {
            table_name,
            _marker: std::marker::PhantomData,
            field_names: get_field_names::<R>(),
        }
    }

    fn query_string(&self) -> String {
        format!(
            "SELECT {} FROM {}",
            self.field_names.join(", "),
            self.table_name
        )
    }
}

impl<'a, R: Default + Serialize> Flowable for TableData<'a, R>
where
    R: From<postgres::Row>,
{
    type Type = R;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = R> {
        state
            .client
            .query_raw(self.query_string().as_str(), Vec::<&str>::new())
            .unwrap()
            .iterator()
            .map(|res| R::from(res.unwrap()))
    }
}

fn get_field_names<T: Serialize + Default>() -> Vec<String> {
    let serialized = ron::ser::to_string(&T::default()).unwrap();
    let val: ron::Value = ron::de::from_str(&serialized).unwrap();
    match val {
        ron::Value::Map(map_val) => map_val
            .iter()
            .map(|(name, _obj)| match (name) {
                ron::Value::String(s) => Some(s.to_string()),
                _ => None,
            })
            .filter_map(|x| x)
            .collect(),
        _ => vec![],
    }
}

#[derive(Debug)]
struct PartialTestRow {
    name: String,
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::*;

    #[test]
    fn test_simple_flow() {
        let table1 = TestTable {};
        let query = Project {
            input: table1,
            projection: |row| PartialTestRow { name: row.name },
        };
        let mut state = computed_state();
        query
            .compute(&mut state)
            .for_each(|row| println!("{:?}", row));
    }

    #[test]
    fn test_simple_select() {
        let table1 = TableData::<TestTableRow>::new("students");
        let query = Select {
            input: table1,
            selecting: |row| PartialTestTableRow {
                name: Some(row.name),
                ..Default::default()
            },
        };
        let mut state = computed_state();
        query
            .compute(&mut state)
            .for_each(|row| println!("{:?}", row));
    }

    #[test]
    fn test_simple_table_data() {
        let table1 = TableData::<TestTableRow>::new("students");
        let mut state = computed_state();
        table1
            .compute(&mut state)
            .for_each(|row| println!("{:?}", row));
    }

    #[test]
    fn test_basic_db_access() {
        let mut client = get_db_client();
        for row in client.query("SELECT id, name FROM students", &[]).unwrap() {
            let id: i32 = row.get(0);
            let name: &str = row.get(1);
            println!("found student: {} {}", id, name);
        }
    }

    #[test]
    fn test_basic_json_serialization() {
        let row = OtherTestTableRow {
            id: 1,
            name: "John".to_string(),
        };
        let serialized = serde_json::to_string(&row).unwrap();
        println!("Serialized row: {}", serialized);
        let deserialized: OtherTestTableRow = serde_json::from_str(&serialized).unwrap();
        assert_eq!(row, deserialized);

        let val: Value = serde_json::from_str(&serialized).unwrap();
        for (name, obj) in val.as_object().unwrap().iter() {
            println!("{} is {:?}", name, obj);
        }
    }

    #[test]
    fn test_basic_ron_serialization() {
        let row = OtherTestTableRow {
            id: 1,
            name: "John".to_string(),
        };
        let serialized = ron::ser::to_string(&row).unwrap();
        println!("Serialized row: {}", serialized);
        let deserialized: OtherTestTableRow = ron::de::from_str(&serialized).unwrap();
        assert_eq!(row, deserialized);

        let val: ron::Value = ron::de::from_str(&serialized).unwrap();
        println!("VALUE {:?}", val);
        if let ron::Value::Map(map_val) = val {
            for (name, obj) in map_val.iter() {
                if let ron::Value::String(s) = name {
                    println!("String key: {}", s);
                }
                println!("{:?} is {:?}", name, obj);
            }
        }
    }

    fn get_db_client() -> Client {
        Client::connect("host=localhost user=ori dbname=qflow", NoTls).unwrap()
    }

    fn computed_state() -> ComputeState {
        ComputeState {
            client: get_db_client(),
        }
    }
}
