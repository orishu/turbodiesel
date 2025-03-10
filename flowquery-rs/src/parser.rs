use pest::Parser;

#[derive(pest_derive::Parser)]
#[grammar = "pest/flowquery.pest"]
pub struct MyParser;

impl MyParser {
    pub fn do_parse(input: &str) {
        let parse = MyParser::parse(Rule::ident, input).unwrap();
        for pair in parse.into_iter() {
            // match the rule, as the rule is an enum
            match (pair.as_rule(), pair.as_span()) {
                (Rule::ident, span) => {
                    println!("{:?}", span);
                    // for each sub-rule, print the inner contents
                    for tag in pair.into_inner() {
                        println!("  {:?}", tag);
                    }
                }
                // as we have  parsed document, which is a top level rule, there
                // cannot be anything else
                _ => unreachable!(),
            }
        }
    }
}
