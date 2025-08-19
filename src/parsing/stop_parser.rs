// 8 file(s).
// File(s) read by the parser:
// BAHNHOF, BFKOORD_LV95, BFKOORD_WGS, BFPRIOS, KMINFO, UMSTEIGB, BHFART_60
// ---
// Files not used by the parser:
// BHFART
use std::{error::Error, vec};
use nom::bytes::complete::take;
use nom::character::complete::space1;
use nom::combinator::rest;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;

use crate::parsing::coordinate_parser::parse as load_coordinates;
use crate::parsing::exchange_priority_parser::parse as load_exchange_priorities;
use crate::parsing::exchange_flag_parser::parse as load_exchange_flags;
use crate::parsing::exchange_time_parser::parse as load_exchange_times;
use crate::parsing::description_parser::parse as load_descriptions;

use crate::{models::{CoordinateSystem, Model, Stop, Version}, parsing::{
    ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition,
    RowParser,
}, storage::ResourceStorage};
use crate::parsing::ParserFnReturn;

type StopStorageAndExchangeTimes = (ResourceStorage<Stop>, (i16, i16));

pub struct StopParser {
    file: String,
    row_parser: RowParser
}
impl StopParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, rest), // Should be 13-62, but some entries go beyond column 62.
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new() -> Self {
        Self {
            file: "BAHNHOF".to_string(),
            row_parser: RowParser::new( {
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_1
                ));
                rows
            })
        }
    }

    fn parse(&self, version: Version, path: &str) -> Result<StopStorageAndExchangeTimes, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let mut data = row_converter(parser)?;
        load_coordinates(version, path, CoordinateSystem::LV95, &mut data)?;
        load_coordinates(version, path, CoordinateSystem::WGS84, &mut data)?;
        load_exchange_priorities(path, &mut data)?;
        load_exchange_flags(path, &mut data)?;
        let default_exchange_time = load_exchange_times(path, &mut data)?;
        load_descriptions(version, path, &mut data)?;

        Ok((ResourceStorage::new(data), default_exchange_time))
    }
}

fn row_converter(parser: FileParser) -> Result<FxHashMap<i32, Stop>, Box<dyn Error>>{
    let data = parser
        .parse()
        .map(|x| x.map(|(_, _, values)| create_instance(values))?)
        .collect::<Result<Vec<_>, _>>()?;
    let data = Stop::vec_to_map(data);
    Ok(data)
}

fn create_instance(mut values: Vec<ParsedValue>) -> Result<Stop, Box<dyn Error>> {
    let id: i32 = values.remove(0).into();
    let designations: String = values.remove(0).into();

    let (name, long_name, abbreviation, synonyms) = parse_designations(designations)?;

    Ok(Stop::new(id, name, long_name, abbreviation, synonyms))
}


pub fn parse(version: Version, path: &str) -> Result<StopStorageAndExchangeTimes, Box<dyn Error>> {
    StopParser::new().parse(version, path)
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

type NameAndAlternatives = (String, Option<String>, Option<String>, Option<Vec<String>>);

fn parse_designations(designations: String) -> Result<NameAndAlternatives, Box<dyn Error>> {
    let designations = designations
        .split('>')
        .filter(|&s| !s.is_empty())
        .map(|s| -> Result<(i32, String), Box<dyn Error>> {
            let s = s.replace('$', "");
            let mut parts = s.split('<');

            let v = parts.next().ok_or("Missing value part")?.to_string();
            let k = parts.next().ok_or("Missing value part")?.parse::<i32>()?;

            Ok((k, v))
        })
        .try_fold(
            FxHashMap::default(),
            |mut acc: std::collections::HashMap<i32, Vec<String>, _>, item| {
                let (k, v) = item?;
                acc.entry(k).or_default().push(v);
                Ok::<_, Box<dyn Error>>(acc)
            },
        )?;

    let name = designations.get(&1).ok_or("Missing stop name")?[0].clone();
    let long_name = designations.get(&2).map(|x| x[0].clone());
    let abbreviation = designations.get(&3).map(|x| x[0].clone());
    let synonyms = designations.get(&4).cloned();

    Ok((name, long_name, abbreviation, synonyms))
}
