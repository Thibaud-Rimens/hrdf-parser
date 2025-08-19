// 1 file(s).
// File(s) read by the parser:
// DURCHBI
use std::error::Error;
use nom::bytes::complete::take;
use nom::character::complete::space1;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{JourneyId, models::{Model, ThroughService}, parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition, RowParser}, storage::ResourceStorage, utils::AutoIncrement};
use crate::parsing::ParserFnReturn;

pub struct ThroughServiceParser {
    file: String,
    row_parser: RowParser
}

impl ThroughServiceParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(6usize),
            preceded(space1, take(6usize)),
            preceded(space1, take(7usize)),
            preceded(space1, take(6usize)),
            preceded(space1, take(6usize)),
            preceded(space1, take(6usize)),
            preceded(space1, take(7usize)),

        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2, data.3, data.4, data.5, data.6]))
    }

    pub fn new() -> Self {
        Self {
            file: "DURCHBI".to_string(),
            row_parser: RowParser::new( {
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer32), // Should be INT16 according to the standard. The standard contains an error. The correct type is INT32.
                        ColumnDefinition::new(ExpectedType::Integer32), // No indication this should be
                    ],
                    Self::get_parser_1
                ));
                rows
            })
        }
    }

    fn parse(
        &self,
        path: &str,
        journeys_pk_type_converter: &FxHashSet<JourneyId>,
    ) -> Result<ResourceStorage<ThroughService>, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let data = row_converter(parser, journeys_pk_type_converter)?;
        Ok(ResourceStorage::new(data))
    }
}

fn row_converter(parser: FileParser, journeys_pk_type_converter: &FxHashSet<JourneyId>) -> Result<FxHashMap<i32, ThroughService>, Box<dyn Error>>{
    let auto_increment = AutoIncrement::new();

    let data = parser
        .parse()
        .map(|x| {
            x.and_then(|(_, _, values)| {
                create_instance(values, &auto_increment, journeys_pk_type_converter)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let data = ThroughService::vec_to_map(data);
    Ok(data)
}

fn create_instance(
    mut values: Vec<ParsedValue>,
    auto_increment: &AutoIncrement,
    journeys_pk_type_converter: &FxHashSet<JourneyId>,
) -> Result<ThroughService, Box<dyn Error>> {
    let journey_1_id: i32 = values.remove(0).into();
    let journey_1_administration: String = values.remove(0).into();
    let journey_1_stop_id: i32 = values.remove(0).into();
    let journey_2_id: i32 = values.remove(0).into();
    let journey_2_administration: String = values.remove(0).into();
    let bit_field_id: i32 = values.remove(0).into();
    let journey_2_stop_id: i32 = values.remove(0).into();

    let _journey_1_id = journeys_pk_type_converter
        .get(&(journey_1_id, journey_1_administration.clone()))
        .ok_or("Unknown legacy ID")?;

    let _journey_2_id = journeys_pk_type_converter
        .get(&(journey_2_id, journey_2_administration.clone()))
        .ok_or("Unknown legacy ID")?;

    if journey_1_stop_id != journey_2_stop_id {
        log::info!("{journey_1_stop_id}, {journey_2_stop_id}");
    }

    Ok(ThroughService::new(
        auto_increment.next(),
        (journey_1_id, journey_1_administration),
        journey_1_stop_id,
        (journey_2_id, journey_2_administration),
        journey_2_stop_id,
        bit_field_id,
    ))
}

pub fn parse(path: &str, journeys_pk_type_converter: &FxHashSet<JourneyId>) -> Result<ResourceStorage<ThroughService>, Box<dyn Error>> {
    ThroughServiceParser::new().parse(path, journeys_pk_type_converter)
}

