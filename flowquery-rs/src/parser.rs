
use pest::Parser;
use crate::pipeline::Pipeline;

#[derive(pest_derive::Parser)]
#[grammar = "pest/flowquery.pest"]
pub struct FlowQueryParser;

impl FlowQueryParser {
    pub fn do_parse(input: &str) {
        let pairs = FlowQueryParser::parse(Rule::query, input).unwrap();
        for pair in pairs.clone() {
            println!("Rule: {:?}, Text: {}", pair.as_rule(), pair.as_str());
            for inner_pair in pair.into_inner() {
                println!("  Inner: {:?}, {}", inner_pair.as_rule(), inner_pair.as_str());
            }
        }

        let pipeline = Pipeline::new(pairs);
        println!("pipeline: {:?}", pipeline)
        // TODO: execute pipeline
    }
}
