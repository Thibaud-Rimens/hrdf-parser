// 1 file(s).
// File(s) read by the parser:
// LINIE
use std::error::Error;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::{rest};
use nom::Parser;
use nom::sequence::{preceded};
use rustc_hash::FxHashMap;
use crate::{models::{Color, Line, Model}, parsing::{
    ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition,
    RowParser,
}, storage::ResourceStorage};

use crate::parsing::ParserFnReturn;


enum RowType {
    RowA = 1,
    RowB = 2,
    RowC = 3,
    RowD = 4,
    RowE = 5,
}

impl TryFrom<i32> for RowType {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RowType::RowA as i32 => Ok(RowType::RowA),
            x if x == RowType::RowB as i32 => Ok(RowType::RowB),
            x if x == RowType::RowC as i32 => Ok(RowType::RowC),
            x if x == RowType::RowD as i32 => Ok(RowType::RowD),
            x if x == RowType::RowE as i32 => Ok(RowType::RowE),
            _ => Err(()),
        }
    }
}

pub struct LineParser {
    file: String,
    row_parser: RowParser
}

impl LineParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((space1, tag("K"), space1), rest),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = preceded((take(8usize), tag("N T"), space1), rest);
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded((take(8usize), tag("F"), space1), take(3usize)),
            preceded(space1, take(3usize)),
            preceded(space1, take(3usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_4(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded((take(8usize), tag("B"), space1), take(3usize)),
            preceded(space1, take(3usize)),
            preceded(space1, take(3usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_5(input: &str) -> ParserFnReturn {
        let mut parser = preceded((take(8usize), tag("L T"), space1), rest);
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    pub fn new() -> Self {
        Self {
            file: "LINIE".to_string(),
            row_parser: RowParser::new({
                let mut rows = vec![];
                // This row is used to create a Line instance.
                rows.push(RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_1
                ));
                // This row contains the short name.
                rows.push(RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ));
                // This row contains the text color.
                rows.push(RowDefinition::new(
                    RowType::RowC as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_3
                ));
                // This row contains the background color.
                rows.push(RowDefinition::new(
                    RowType::RowD as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_4
                ));
                // This row contains the short name.
                rows.push(RowDefinition::new(
                    RowType::RowE as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_5
                ));
                rows
            })
        }
    }

    pub fn parse(&self, path: &str) -> Result<ResourceStorage<Line>, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let data = row_converter(parser)?;
        Ok(ResourceStorage::new(data))
    }
}

fn row_converter(
    parser: FileParser,
) -> Result<FxHashMap<i32, Line>, Box<dyn Error>>  {
    let mut data = Vec::new();
    for x in parser.parse() {
        let (id, _, values) = x?;
        match id.try_into() {
            Ok(RowType::RowA) => data.push(create_instance(values)),
            _ => {
                let line = data.last_mut().ok_or("Type A row missing.")?;

                match id.try_into() {
                    Ok(RowType::RowB) => set_short_name(values, line),
                    Ok(RowType::RowC) => set_text_color(values, line),
                    Ok(RowType::RowD) => set_background_color(values, line),
                    Ok(RowType::RowE) => set_long_name(values, line),
                    _ => unreachable!(),
                }
            }
        }
    }

    let data = Line::vec_to_map(data);
    Ok(data)
}

fn create_instance(
    mut values: Vec<ParsedValue>,
) -> Line {
    let id: i32 = values.remove(0).into();
    let name: String = values.remove(0).into();
    Line::new(id, name)
}

pub fn parse(path: &str) -> Result<ResourceStorage<Line>, Box<dyn Error>> {
    LineParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn set_short_name(mut values: Vec<ParsedValue>, line: &mut Line) {
    let short_name: String = values.remove(0).into();

    line.set_short_name(short_name);
}

fn set_long_name(mut values: Vec<ParsedValue>, line: &mut Line) {
    let long_name: String = values.remove(0).into();

    line.set_long_name(long_name);
}

fn set_text_color(mut values: Vec<ParsedValue>, line: &mut Line) {
    let r: i16 = values.remove(0).into();
    let g: i16 = values.remove(0).into();
    let b: i16 = values.remove(0).into();

    line.set_text_color(Color::new(r, g, b));
}

fn set_background_color(mut values: Vec<ParsedValue>, line: &mut Line) {
    let r: i16 = values.remove(0).into();
    let g: i16 = values.remove(0).into();
    let b: i16 = values.remove(0).into();

    line.set_background_color(Color::new(r, g, b));
}
