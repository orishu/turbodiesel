
mod parser;

use crate::parser::MyParser;

fn main() {
    println!("Hello, world!");
    let input: &str = "abcd";
    MyParser::do_parse(input);
}
