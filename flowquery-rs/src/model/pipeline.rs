use core::fmt;
use std::collections::HashMap;

use postgres::fallible_iterator::FallibleIterator;

use postgres::{Client, NoTls};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

struct ComputeState {
    client: Client,
}

trait Flowable {
    type Type;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = Self::Type>;
}

enum ColumnSelection {
    All,
    Selected(Vec<(String, String)>),
}

trait Sqlable {
    fn execute(&self, state: &mut ComputeState) -> impl Iterator<Item = postgres::Row>;

    fn source_name(&self) -> String;

    fn selected_columns(&self) -> ColumnSelection;
}

trait Columnar {
    fn column_names(&self) -> Vec<String>;
}

#[derive(Debug)]
struct Table {
    table_name: String,
}

impl Table {
    fn new(table_name: &str) -> Self {
        Table {
            table_name: table_name.to_string(),
        }
    }
}

impl Sqlable for Table {
    fn execute(&self, state: &mut ComputeState) -> impl Iterator<Item = postgres::Row> {
        let query_string = format!("SELECT * FROM {}", self.table_name.to_string());
        state
            .client
            .query_raw(query_string.as_str(), Vec::<&str>::new())
            .unwrap()
            .iterator()
            .map(|r| r.unwrap())
    }

    fn source_name(&self) -> String {
        self.table_name.clone()
    }

    fn selected_columns(&self) -> ColumnSelection {
        ColumnSelection::All
    }
}

#[derive(Debug)]
struct Select1<'a> {
    source: Table,
    select_sql_expressions: HashMap<&'a str, &'a str>,
}

impl<'a> Select1<'a> {
    fn new(source: Table, sql_expressions: HashMap<&'a str, &'a str>) -> Self {
        Select1 {
            source,
            select_sql_expressions: sql_expressions,
        }
    }
}

impl<'a> Columnar for Select1<'a> {
    fn column_names(&self) -> Vec<String> {
        self.select_sql_expressions
            .keys()
            .into_iter()
            .map(|x| x.clone().to_string())
            .collect()
    }
}

impl<'a> Sqlable for Select1<'a> {
    fn execute(&self, state: &mut ComputeState) -> impl Iterator<Item = postgres::Row> {
        let column_defs = self
            .select_sql_expressions
            .iter()
            .map(|(key, value)| format!("{} AS {}", *value, *key))
            .collect::<Vec<_>>();
        let query_string = format!(
            "SELECT {} FROM {}",
            column_defs.join(", "),
            self.source.table_name
        );
        state
            .client
            .query_raw(query_string.as_str(), Vec::<&str>::new())
            .unwrap()
            .iterator()
            .map(|r| r.unwrap())
    }

    fn source_name(&self) -> String {
        self.source.source_name()
    }

    fn selected_columns(&self) -> ColumnSelection {
        ColumnSelection::Selected(
            self.select_sql_expressions.iter()
            .map(
                |(k, v)| (k.to_string(), v.to_string()))
            .collect())
    }
}

struct RowReader<S: Sqlable, T: From<postgres::Row>> {
    source: S,
    _marker: std::marker::PhantomData<T>,
}

impl<S: Sqlable, T: From<postgres::Row>> RowReader<S, T> {
    fn new(source: S) -> Self {
        RowReader {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: Sqlable, T: From<postgres::Row>> Flowable for RowReader<S, T> {
    type Type = T;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = T> {
        self.source.execute(state).map(|row| T::from(row))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TestTableRow {
    id: i64,
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct OtherTestTableRow {
    id: i64,

    #[serde(rename = "the_name")]
    name: String,
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
struct Select<'a, O: Default + Serialize + From<postgres::Row>> {
    source: TableData<'a, O>, // TODO: rather than store a TableData, store a generic bound ColumnSelectable trait
}

impl<'a, O: Default + Serialize + From<postgres::Row>> Select<'a, O> {
    // TODO: rather than pass a TableData as input, pass a generic bound ColumnSelectable trait

    fn new<I: Default + Serialize>(
        input: TableData<'a, I>,
        select_sql_expressions: HashMap<&'a str, &'a str>,
    ) -> Self {
        Self {
            source: input.select_columns(select_sql_expressions),
        }
    }
}

impl<'a, O: Default + Serialize + From<postgres::Row>> Flowable for Select<'a, O> {
    type Type = O;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = O> {
        self.source.compute(state)
    }
}

#[derive(Debug)]
struct TableData<'a, R: Default + Serialize> {
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

    fn select_columns<O: Default + Serialize>(
        &self,
        column_selection: HashMap<&'a str, &'a str>,
    ) -> TableData<'a, O> {
        TableData::<'a, O>::new(self.table_name) // TODO: actually select the columns
    }

    fn query_string(&self) -> String {
        format!(
            "SELECT {} FROM {}", // TODO: sanitize
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

#[derive(Default, Debug, Serialize, Deserialize)]
struct PartialTestRow {
    name: String,
}

impl From<TestTableRow> for PartialTestRow {
    fn from(row: TestTableRow) -> Self {
        PartialTestRow { name: row.name }
    }
}

impl From<postgres::Row> for PartialTestRow {
    fn from(row: postgres::Row) -> Self {
        PartialTestRow {
            name: row.get("name"),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::hash;

    use super::*;
    use map_macro::hash_map;

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
    fn test_simple_select_compute() {
        let table1 = TableData::<TestTableRow>::new("students");
        let query = Select::<PartialTestRow>::new(table1, HashMap::new());
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

        let val2: ron::Value = ron::de::from_str(&serialized).unwrap();
        let deconstructed: OtherTestTableRow = val2.into_rust().unwrap();
        println!("Deconstructed {:?}", deconstructed);
        assert_eq!(row, deconstructed);
    }

    fn get_db_client() -> Client {
        Client::connect("host=localhost user=ori dbname=qflow", NoTls).unwrap()
    }

    fn computed_state() -> ComputeState {
        ComputeState {
            client: get_db_client(),
        }
    }

    #[test]
    fn test_row_reader_from_table() {
        let table1 = Table::new("students");
        let row_reader = RowReader::<_, TestTableRow>::new(table1);
        let mut state = computed_state();
        row_reader
            .compute(&mut state)
            .for_each(|row| println!("{:?}", row));
    }

    #[test]
    fn test_row_reader_from_select() {
        let table1 = Table::new("students");
        let row_reader = RowReader::<_, TestTableRow>::new(Select1::new(
            table1,
            hash_map! {
                "id" => "id",
                "name" => "concat(name, '-', cast(id as text))",
            },
        ));
        let mut state = computed_state();
        row_reader
            .compute(&mut state)
            .for_each(|row| println!("{:?}", row));
    }
}
