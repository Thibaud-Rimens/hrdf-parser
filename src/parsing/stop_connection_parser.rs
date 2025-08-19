// 1 file(s).
// File(s) read by the parser:
// METABHF
use std::error::Error;
use std::sync::Arc;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::rest;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;

use crate::{models::{Model, StopConnection}, parsing::{
    ColumnDefinition, ExpectedType, FileParser,
    ParsedValue, RowDefinition, RowParser,
}, storage::ResourceStorage, utils::AutoIncrement, Line};
use crate::parsing::ParserFnReturn;

enum RowType {
    RowA = 1,
    RowB = 2,
    RowC = 3,
}

impl TryFrom<i32> for RowType {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RowType::RowA as i32 => Ok(RowType::RowA),
            x if x == RowType::RowB as i32 => Ok(RowType::RowB),
            x if x == RowType::RowC as i32 => Ok(RowType::RowC),
            _ => Err(()),
        }
    }
}

pub struct StopConnectionParser {
    file: String,
    row_parser: Arc<RowParser>
}

impl StopConnectionParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, take(7usize)),
            preceded(space1, take(3usize))
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = preceded(tag("#"), preceded(space1, take(2usize)));
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            tag(":")
        );
        let (i2, _) = parser.parse(input)?;
        Ok((i2, vec![]))
    }

    pub fn new() -> Self {
        Self {
            file: "METABHF".to_string(),
            row_parser: Arc::new(RowParser::new(vec![
                RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_1
                ),
                // This row contains the attributes.
                RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ),
                // This row is ignored.
                RowDefinition::new(
                    RowType::RowC as i32,
                    vec![],
                    Self::get_parser_3,
                ),
            ]))
        }
    }

    fn row_converter(
        &self,
        parser: FileParser,
        attributes_pk_type_converter: &FxHashMap<String, i32>,
    ) -> Result<FxHashMap<i32, StopConnection>, Box<dyn Error>>  {
        let auto_increment = AutoIncrement::new();
        let mut data = Vec::new();
        for x in parser.parse() {
            let (id, _, values) = x?;
            match id.try_into() {
                Ok(RowType::RowA) => {
                    if id == RowType::RowA as i32 {
                        data.push(self.create_instance(values, &auto_increment));
                    }
                }
                _ => {
                    let stop_connection = data.last_mut().ok_or("Type A row missing.")?;
                    match id.try_into() {
                        Ok(RowType::RowB) => set_attribute(values, stop_connection, attributes_pk_type_converter)?,
                        Ok(RowType::RowC) => {},
                        _ => unreachable!()
                    }
                }

            }
        }
        let data = StopConnection::vec_to_map(data);
        Ok(data)
    }

    pub fn parse(
        &self,
        path: &str,
        attributes_pk_type_converter: &FxHashMap<String, i32>,
    ) -> Result<ResourceStorage<StopConnection>, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), Arc::clone(&self.row_parser))?;
        let data = self.row_converter(parser, attributes_pk_type_converter)?;
        Ok(ResourceStorage::new(data))
    }

    fn create_instance(&self, mut values: Vec<ParsedValue>, auto_increment: &AutoIncrement) -> StopConnection {
        let stop_id_1: i32 = values.remove(0).into();
        let stop_id_2: i32 = values.remove(0).into();
        let duration: i16 = values.remove(0).into();
        StopConnection::new(auto_increment.next(), stop_id_1, stop_id_2, duration)
    }
}

pub fn parse(
    path: &str,
    attributes_pk_type_converter: &FxHashMap<String, i32>,
) -> Result<ResourceStorage<StopConnection>, Box<dyn Error>> {
    StopConnectionParser::new().parse(path, attributes_pk_type_converter)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn set_attribute(
    mut values: Vec<ParsedValue>,
    current_instance: &mut StopConnection,
    attributes_pk_type_converter: &FxHashMap<String, i32>,
) -> Result<(), Box<dyn Error>> {
    let attribute_designation: String = values.remove(0).into();
    let attribute_id = *attributes_pk_type_converter
        .get(&attribute_designation)
        .ok_or("Unknown legacy ID")?;
    current_instance.set_attribute(attribute_id);
    Ok(())
}
