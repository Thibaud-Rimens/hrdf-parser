// 4 file(s).
// File(s) read by the parser:
// BETRIEB_DE, BETRIEB_EN, BETRIEB_FR, BETRIEB_IT
use std::error::Error;
use std::sync::Arc;
use chrono::NaiveDate;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::rest;
use nom::Parser;
use nom::sequence::preceded;
use regex::Regex;
use rustc_hash::FxHashMap;

use crate::{models::{Language, Model, TransportCompany}, parsing::{
    ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition,
    RowParser,
}, storage::ResourceStorage, TimetableMetadataEntry};
use crate::parsing::ParserFnReturn;
use crate::utils::AutoIncrement;

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

pub struct TransportCompanyParser {
    files: Vec<String>,
    languages: Vec<Language>,
    row_parser: Arc<RowParser>
}

impl TransportCompanyParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(5usize),
            preceded(tag("K"), preceded(space1, rest))
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(5usize),
            preceded(tag(":"), preceded(space1, rest))
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded(take(5usize), tag("N")),
            preceded(space1, rest)
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new() -> Self {
        Self {
            files: vec!["BETRIEB_DE".to_string(), "BETRIEB_EN".to_string(), "BETRIEB_FR".to_string(), "BETRIEB_IT".to_string()],
            languages: vec![Language::German, Language::English, Language::French, Language::Italian],
            row_parser: Arc::new(RowParser::new( {
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_1
                ));
                rows.push(RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ));
                rows.push(RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_3
                ));
                rows
            }))
        }
    }

    fn row_converter(&self, parser: FileParser, data: &mut FxHashMap<i32, TransportCompany>, language: Language, ) -> Result<(), Box<dyn Error>> {
        parser.parse().try_for_each(|x| {
            let (id, _, values) = x?;
            if id == RowType::RowA as i32 {
                set_designations(values, data, language)?
            }
            Ok(())
        })
    }

    pub fn parse(&self, path: &str) -> Result<ResourceStorage<TransportCompany>, Box<dyn Error>> {
        for file in self.files.iter() {
            log::info!("Parsing {}...", file);
        }

        let parser = FileParser::new(&format!("{}/{}", path, self.files[0]), Arc::clone(&self.row_parser))?;
        let data = parser
            .parse()
            .map(|x| {
                x.map(|(id, _, values)| {
                    match id.try_into() {
                        Ok(RowType::RowA) => None,
                        Ok(RowType::RowB) => Some(self.create_instance(values)),
                        Ok(RowType::RowC) => None, // TODO we should probably add an explicit treatment for the sboid
                        _ => unreachable!(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // If there are no errors, "None" values are removed.
        let data = data.into_iter().flatten().collect();
        let mut data = TransportCompany::vec_to_map(data);

        for language in &self.languages {
            self.load_designations(path, &mut data, *language)?;
        }

        Ok(ResourceStorage::new(data))
    }

    fn create_instance(&self, mut values: Vec<ParsedValue>) -> TransportCompany {
        let id: i32 = values.remove(0).into();
        let administrations = values.remove(0).into();
        let administrations = parse_administrations(administrations);

        TransportCompany::new(id, administrations)
    }

    fn load_designations(
        &self,
        path: &str,
        data: &mut FxHashMap<i32, TransportCompany>,
        language: Language,
    ) -> Result<(), Box<dyn Error>> {
        let filename = match language {
            Language::German => "BETRIEB_DE",
            Language::English => "BETRIEB_EN",
            Language::French => "BETRIEB_FR",
            Language::Italian => "BETRIEB_IT",
        };
        let parser = FileParser::new(&format!("{path}/{filename}"), Arc::clone(&self.row_parser))?;

        parser.parse().try_for_each(|x| {
            let (id, _, values) = x?;
            if id == RowType::RowA as i32 {
                set_designations(values, data, language)?
            }
            Ok(())
        })
    }
}

pub fn parse(path: &str) -> Result<ResourceStorage<TransportCompany>, Box<dyn Error>> {
    TransportCompanyParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn set_designations(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, TransportCompany>,
    language: Language,
) -> Result<(), Box<dyn Error>> {
    let id: i32 = values.remove(0).into();
    let designations = values.remove(0).into();

    let (short_name, long_name, full_name) = parse_designations(designations);

    let transport_company = data.get_mut(&id).ok_or("Unknown ID")?;
    transport_company.set_short_name(language, &short_name);
    transport_company.set_long_name(language, &long_name);
    transport_company.set_full_name(language, &full_name);

    Ok(())
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

fn parse_administrations(administrations: String) -> Vec<String> {
    administrations
        .split_whitespace()
        .map(|s| s.to_owned())
        .collect()
}

fn parse_designations(designations: String) -> (String, String, String) {
    // unwrap: The creation of this regular expression will never fail.
    let re = Regex::new(r" ?([KLV]) ").unwrap();
    let designations: Vec<String> = re
        .split(&designations)
        .map(|s| s.chars().filter(|&c| c != '"').collect())
        .collect();

    let short_name = designations[0].to_owned();
    let long_name = designations[1].to_owned();
    let full_name = designations[2].to_owned();

    (short_name, long_name, full_name)
}