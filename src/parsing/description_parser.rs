use std::error::Error;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::rest;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;
use crate::parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, ParserFnReturn, RowDefinition, RowParser};
use crate::{Stop, Version};

enum RowType {
    RowA = 1,
    RowB = 2,
    RowC = 3,
    RowD = 4,
    RowE = 5,
    RowF = 6,
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
            x if x == RowType::RowF as i32 => Ok(RowType::RowF),
            _ => Err(()),
        }
    }
}

pub struct DescriptionParser {
    file: String,
    row_parser: RowParser
}

impl DescriptionParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = tag("%");
        let (i2, _) = parser.parse(input)?;
        Ok((i2, vec![]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((space1, tag("B"), space1), take(2usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((take(3usize), tag("A"), space1), rest),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_4(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((take(3usize), tag("a"), space1), rest),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_5(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((space1, tag("L"), space1), take(2usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_6(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded((space1, tag("I"), space1), take(2usize)),
            preceded(space1, take(9usize))

        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new(version: Version) -> Self {
        Self {
            file: match version {
                Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 => "BHFART_60",
                Version::V_5_40_41_2_0_7 => "BHFART",
            }.to_string(),
            row_parser: RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    RowType::RowA as i32,
                    vec![],
                    Self::get_parser_1,
                ));
                rows.push(RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_2,
                ));
                rows.push(RowDefinition::new(
                    RowType::RowC as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_3,
                ));
                rows.push(RowDefinition::new(
                    RowType::RowD as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_4,
                ));
                rows.push(RowDefinition::new(
                    RowType::RowE as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_5,
                ));
                rows.push(RowDefinition::new(
                    RowType::RowF as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer32),
                    ],
                    Self::get_parser_6,
                ));
                rows
            })
        }
    }

    fn parse(
        &self,
        path: &str,
        data: &mut FxHashMap<i32, Stop>,
    ) -> Result<(), Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;

        parser.parse().try_for_each(|x| {
            let (id, _, values) = x?;
            match id.try_into() {
                Ok(RowType::RowA) => {}
                Ok(RowType::RowB) => set_restrictions(values, data)?,
                Ok(RowType::RowC) => set_sloid(values, data)?,
                Ok(RowType::RowD) => add_boarding_area(values, data)?,
                Ok(RowType::RowE) => {} // TODO: add possibility to use Land data
                Ok(RowType::RowF) => {} // TODO: add possibility to use KT information and the associated number
                _ => unreachable!(),
            }
            Ok(())
        })
    }
}

pub fn parse(
    version: Version,
    path: &str,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    DescriptionParser::new(version).parse(path, data)
}

fn set_restrictions(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let restrictions: i16 = values.remove(0).into();

    if let Some(stop) = data.get_mut(&stop_id) {
        stop.set_restrictions(restrictions);
    } else {
        log::info!("Unknown ID: {stop_id} for restrictions");
    }

    Ok(())
}

fn set_sloid(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let sloid: String = values.remove(0).into();

    if let Some(stop) = data.get_mut(&stop_id) {
        stop.set_sloid(sloid);
    } else {
        log::info!("Unknown ID: {stop_id} for sloid");
    }

    Ok(())
}

fn add_boarding_area(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let sloid: String = values.remove(0).into();

    if let Some(stop) = data.get_mut(&stop_id) {
        stop.add_boarding_area(sloid);
    } else {
        log::info!("Unknown ID: {stop_id} for boarding area");
    }

    Ok(())
}