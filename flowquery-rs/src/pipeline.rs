use crate::parser::Rule;

#[derive(Debug)]
pub struct Query {
    pipeline: Pipeline,
}

#[derive(Debug)]
pub struct Pipeline {
    source: TableRef,
    transforms: Vec<TransformationClass>,
}

#[derive(Debug)]
enum TransformationClass {
    Pipe(PipeTransform),
    SideEffect(SideEffectTransform),
}

#[derive(Debug)]
pub struct PipeTransform {}

#[derive(Debug)]
pub struct SideEffectTransform {}

#[derive(Debug)]
pub enum ParseError {
    NotImplemented,
    UnexpectedToken,
    MissingToken,
    MissingRule,
}

#[derive(Debug)]
struct TableRef {
    table_name: String,
    alias: Option<String>,
}

pub trait Parsable: Sized {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError>;
}

impl Parsable for TableRef {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError> {
        let mut iter = pairs;
        let table_name = iter
            .next()
            .ok_or(ParseError::MissingToken)?
            .as_str()
            .to_string();
        let alias = iter.next().and_then(|p| Some(p.as_str().to_string()));
        Ok(TableRef { table_name, alias })
    }
}

impl Parsable for Query {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError> {
        pairs
            .filter(|p| p.as_rule() == Rule::pipeline)
            .map(|p| {
                let pipeline = Pipeline::parse(p.into_inner())?;
                Ok(Query { pipeline })
            })
            .next()
            .ok_or(ParseError::MissingToken)?
    }
}

impl Parsable for Pipeline {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError> {
        Err(ParseError::NotImplemented)
    }
}

/*
impl Pipeline {
    pub fn new(pairs: pest::iterators::Pairs<'_, Rule>) -> Self {
        //let mut stages = Vec::new();
        let mut table: Option<TableRef> = None;
        for pair in pairs {
            match (pair.as_rule(), pair.into_inner()) {
                (Rule::query, inner) => {
                    for query_pair in inner {
                        match (query_pair.as_rule(), query_pair.into_inner()) {
                            (Rule::pipeline, inner) => {
                                for pipeline_pair in inner {
                                    match (pipeline_pair.as_rule(), pipeline_pair.into_inner()) {
                                        (Rule::source, mut inner) => {
                                            let table_ref = TableRef::parse(inner.next().unwrap());
                                            println!("Table ref: {:?}", table_ref);
                                            table = Some(table_ref);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
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

    */
