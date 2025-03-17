
mod parser;
mod pipeline;
mod model;

fn main() {
    println!("Hello, world!");
    let query = r#"
        table1 as t1
        |> select(column1, column2)
        |> filter(column1 = "value")
    "#;
    parser::FlowQueryParser::do_parse(query);
}
