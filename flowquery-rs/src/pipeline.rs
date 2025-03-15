use crate::parser::Rule;
use pest::iterators::Pair;

/// Represents a parsed query containing a pipeline.
#[derive(Debug)]
pub struct Query {
    pipeline: Pipeline,
}

/// Represents a pipeline with a source and a list of transformations.
#[derive(Debug)]
pub struct Pipeline {
    source: Source,
    transforms: Vec<TransformationClass>,
}

/// Represents a source in the pipeline.
#[derive(Debug)]
pub struct Source {
    source_option: SourceClass,
}

/// Enumerates possible source types.
#[derive(Debug)]
pub enum SourceClass {
    TableRef(TableRef),
}

/// Enumerates transformation classes, distinguishing pipe and side-effect transforms.
#[derive(Debug)]
pub enum TransformationClass {
    Pipe(PipeTransform),
    SideEffect(SideEffectTransform),
}

/// Placeholder for pipe transformations (e.g., select, filter).
#[derive(Debug)]
pub struct PipeTransform {}

/// Placeholder for side-effect transformations (e.g., update).
#[derive(Debug)]
pub struct SideEffectTransform {}

/// Custom error type for parsing failures.
#[derive(Debug)]
pub enum ParseError {
    NotImplemented,
    UnexpectedToken,
    MissingToken,
}

/// Represents a table reference with a name and an optional alias.
#[derive(Debug)]
pub struct TableRef {
    table_name: String,
    alias: Option<String>,
}

/// Trait for types that can be parsed from a Pest `Pair`.
pub trait Parsable: Sized {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError>;
}

impl Parsable for TableRef {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        if pair.as_rule() != Rule::table_ref {
            return Err(ParseError::UnexpectedToken);
        }
        let mut inner = pair.into_inner();
        let table_name_pair = inner.next().ok_or(ParseError::MissingToken)?;
        if table_name_pair.as_rule() != Rule::ident {
            return Err(ParseError::UnexpectedToken);
        }
        let table_name = table_name_pair.as_str().to_string();
        let alias = inner
            .next()
            .map(|alias_pair| {
                if alias_pair.as_rule() != Rule::ident {
                    return Err(ParseError::UnexpectedToken);
                }
                Ok(alias_pair.as_str().to_string())
            })
            .transpose()?;
        Ok(TableRef { table_name, alias })
    }
}

impl Parsable for Query {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        if pair.as_rule() != Rule::query {
            return Err(ParseError::UnexpectedToken);
        }
        let pipeline_pair = pair.into_inner().next().ok_or(ParseError::MissingToken)?;
        let pipeline = Pipeline::parse(pipeline_pair)?;
        Ok(Query { pipeline })
    }
}

impl Parsable for Source {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        if pair.as_rule() != Rule::source {
            return Err(ParseError::UnexpectedToken);
        }
        let inner_pair = pair.into_inner().next().ok_or(ParseError::MissingToken)?;
        let source_option = match inner_pair.as_rule() {
            Rule::table_ref => SourceClass::TableRef(TableRef::parse(inner_pair)?),
            _ => return Err(ParseError::UnexpectedToken),
        };
        Ok(Source { source_option })
    }
}

impl Parsable for Pipeline {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        if pair.as_rule() != Rule::pipeline {
            return Err(ParseError::UnexpectedToken);
        }
        let mut inner = pair.into_inner();
        let source_pair = inner.next().ok_or(ParseError::MissingToken)?;
        let source = Source::parse(source_pair)?;
        let transforms = inner
            .map(TransformationClass::parse)
            .collect::<Result<Vec<_>, ParseError>>()?;
        Ok(Pipeline { source, transforms })
    }
}

impl Parsable for TransformationClass {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        match pair.as_rule() {
            Rule::pipe_transform => {
                let inner_pair = pair.into_inner().next().ok_or(ParseError::MissingToken)?;
                Ok(TransformationClass::Pipe(PipeTransform::parse(inner_pair)?))
            }
            Rule::side_effect_transform => {
                let inner_pair = pair.into_inner().next().ok_or(ParseError::MissingToken)?;
                Ok(TransformationClass::SideEffect(SideEffectTransform::parse(
                    inner_pair,
                )?))
            }
            _ => Err(ParseError::UnexpectedToken),
        }
    }
}

// Placeholder implementations for empty structs
impl Parsable for PipeTransform {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        // TODO: Implement parsing when fields are added
        Ok(PipeTransform {})
    }
}

impl Parsable for SideEffectTransform {
    fn parse(pair: Pair<'_, Rule>) -> Result<Self, ParseError> {
        // TODO: Implement parsing when fields are added
        Ok(SideEffectTransform {})
    }
}
