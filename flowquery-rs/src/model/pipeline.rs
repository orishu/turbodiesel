
use partially::Partial;
use postgres::fallible_iterator::FallibleIterator;

use postgres::{Client, NoTls};

use partially_derive::Partial;

struct ComputeState {
    client: Client,
}

trait Flowable {
    type Type;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = Self::Type>;
}

#[derive(Debug, Partial)]
#[partially(derive(Debug, Default))]
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
}

impl<'a, R> TableData<'a, R> {
    fn new(table_name: &'a str) -> Self {
        TableData {
            table_name,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, R> Flowable for TableData<'a, R>
where
    R: From<postgres::Row>,
{
    type Type = R;

    fn compute(&self, state: &mut ComputeState) -> impl Iterator<Item = R> {
        state
            .client
            .query_raw(
                ("select * from ".to_string() + self.table_name).as_str(),
                Vec::<&str>::new(),
            )
            .unwrap()
            .iterator()
            .map(|res| R::from(res.unwrap()))
    }
}

#[derive(Debug)]
struct PartialTestRow {
    name: String,
}

#[cfg(test)]
mod tests {
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

    fn get_db_client() -> Client {
        Client::connect("host=localhost user=ori dbname=qflow", NoTls).unwrap()
    }

    fn computed_state() -> ComputeState {
        ComputeState {
            client: get_db_client(),
        }
    }
}
