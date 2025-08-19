use std::error::Error;
use nom::bytes::complete::take;
use nom::character::complete::{char};
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;
use crate::parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, ParserFnReturn, RowDefinition, RowParser};
use crate::{CoordinateSystem, Coordinates, Stop, Version};

pub struct CoordinateParser {
    files: Vec<String>,
    row_parser: RowParser
}

impl CoordinateParser {
    fn get_parser_2_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(char(' '), take(10usize)),
            preceded(char(' '), take(10usize)),
            preceded(char(' '), take(6usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2, data.3]))
    }

    fn get_parser_2_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(char(' '), take(11usize)),
            preceded(char(' '), take(11usize)),
            preceded(char(' '), take(7usize))

        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2, data.3]))
    }

    pub fn new(version: Version) -> Self {
        Self {
            files: vec!["BFKOORD_LV95".to_string(), "BFKOORD_WGS".to_string()],
            row_parser: RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Float),
                        ColumnDefinition::new(ExpectedType::Float),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    match version {
                        Version::V_5_40_41_2_0_4 => Self::get_parser_2_1,
                        Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 | Version::V_5_40_41_2_0_7 => Self::get_parser_2_2
                    }
                ));
                rows
            })
        }
    }

    fn parse(
        &self,
        path: &str,
        coordinate_system: CoordinateSystem,
        data: &mut FxHashMap<i32, Stop>,
    ) -> Result<(), Box<dyn Error>> {
        let filename = match coordinate_system {
            CoordinateSystem::LV95 => self.files[0].clone(),
            CoordinateSystem::WGS84 => self.files[1].clone(),
        };
        log::info!("Parsing {}...", filename);
        let parser = FileParser::new(&format!("{path}/{filename}"), self.row_parser.clone())?;

        parser.parse().try_for_each(|x| {
            let (_, _, values) = x?;
            set_coordinates(values, coordinate_system, data)?;
            Ok(())
        })
    }
}

pub fn parse(
    version: Version,
    path: &str,
    coordinate_system: CoordinateSystem,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    CoordinateParser::new(version).parse(path, coordinate_system, data)
}

fn set_coordinates(
    mut values: Vec<ParsedValue>,
    coordinate_system: CoordinateSystem,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let mut xy1: f64 = values.remove(0).into();
    let mut xy2: f64 = values.remove(0).into();
    // Altitude is not stored, as it is not provided for 95% of stops.
    let _altitude: i16 = values.remove(0).into();

    if coordinate_system == CoordinateSystem::WGS84 {
        // WGS84 coordinates are stored in reverse order for some unknown reason.
        (xy1, xy2) = (xy2, xy1);
    }

    let stop = data.get_mut(&stop_id).ok_or("Unknown ID")?;
    let coordinate = Coordinates::new(coordinate_system, xy1, xy2);

    match coordinate_system {
        CoordinateSystem::LV95 => stop.set_lv95_coordinates(coordinate),
        CoordinateSystem::WGS84 => stop.set_wgs84_coordinates(coordinate),
    }

    Ok(())
}