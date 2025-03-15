use crate::parser::Rule;

#[derive(Debug)]
pub struct Query {
    pipeline: Pipeline,
}

#[derive(Debug)]
pub struct Pipeline {
    source: Source,
    transforms: Vec<TransformationClass>,
}

#[derive(Debug)]
pub struct Source {
    source_option: SourceClass,
}

#[derive(Debug)]
enum SourceClass {
    TableRef(TableRef),
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
        let mut iter = pairs.filter(|p| p.as_rule() == Rule::ident);
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

impl Parsable for Source {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError> {
        let mut iter = pairs;
        let item = iter.next().ok_or(ParseError::MissingToken)?;
        let table_ref = match (item.as_rule(), item.into_inner()) {
            (Rule::table_ref, inner) => Ok(SourceClass::TableRef(TableRef::parse(inner)?)),
            _ => Err(ParseError::UnexpectedToken)?,
        }?;
        Ok(Source {
            source_option: table_ref,
        })
    }
}

impl Parsable for Pipeline {
    fn parse(pairs: pest::iterators::Pairs<'_, Rule>) -> Result<Self, ParseError> {
        let mut iter = pairs;
        let first_child = iter.next().ok_or(ParseError::MissingToken)?;
        let source = Source::parse(first_child.into_inner())?;
        let transforms: Result<Vec<TransformationClass>, ParseError> = iter
            .map(|t| match (t.as_rule(), t.into_inner()) {
                (Rule::pipe_transform, inner) => Ok(TransformationClass::Pipe(PipeTransform {})),
                (Rule::side_effect_transform, inner) => {
                    Ok(TransformationClass::SideEffect(SideEffectTransform {}))
                }
                _ => Err(ParseError::UnexpectedToken)?,
            })
            .collect();
        Ok(Pipeline {
            source,
            transforms: transforms?,
        })
    }
}
