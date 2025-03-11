use crate::parser::FlowQueryParser;
use crate::parser::Rule;

pub trait Stage {}

#[derive(Debug)]
pub struct Pipeline {
    source: TableRef,
}

trait Parsable {
    fn parse(pair: pest::iterators::Pair<'_, Rule>) -> Self;
}

#[derive(Debug)]
struct TableRef {
    table_name: String,
    alias: Option<String>,
}

impl Parsable for TableRef {
    fn parse(pair: pest::iterators::Pair<'_, Rule>) -> Self {
        println!("ORIORI {:?}", pair.as_rule());
        let mut iter = pair.into_inner();
        let table_name = iter.next().unwrap().as_str().to_string();
        let alias = iter.next().and_then(|p| Some(p.as_str().to_string()));
        TableRef { table_name, alias }
    }
}

impl Pipeline {
    pub fn new(pairs: pest::iterators::Pairs<'_, Rule>) -> Self {
        //let mut stages = Vec::new();
        let mut table: Option<TableRef> = None;
        for pair in pairs {
            match (pair.as_rule(), pair.into_inner())  {
                (Rule::query, inner) => {
                    for query_pair in inner {
                        match (query_pair.as_rule(), query_pair.into_inner())  {
                            (Rule::pipeline, inner) => {
                                for pipeline_pair in inner {
                                    match (pipeline_pair.as_rule(), pipeline_pair.into_inner())  {
                                        (Rule::source, mut inner) => {
                                            let table_ref = TableRef::parse(inner.next().unwrap());
                                            println!("Table ref: {:?}", table_ref);
                                            table = Some(table_ref);
                                        },
                                        _ => {}
                                    }
                                }
                            },
                            _ => {}
                        }
                    }
                }
                _ => {},
            }
        }

        println!("Table: {:?}", table);
        /*
        let mut iter = pairs.into_iter();
        let source = iter.next().unwrap();
        let table_ref = TableRef::parse(source);


        for pair in pipeline {
            match (pair.as_rule(), pair.as_span())  {
                (Rule::source, span) => {
                    println!("Source found at {:?}", span);
                },
                (Rule::pipe_transform, span) => {
                    println!("Pipe Transform found at {:?}", span);
        for pair in pairs {
            match (pair.as_rule(), pair.as_span())  {
                (Rule::query, span) => {
                    println!("Query found at {:?}", span);
                }
                _ => unreachable!(),
            }
        }
        */
        Pipeline {
            source: table.unwrap(),
        }
    }
}
