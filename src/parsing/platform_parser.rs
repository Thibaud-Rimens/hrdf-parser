// 3 file(s).
// File(s) read by the parser:
// GLEIS, GLEIS_LV95, GLEIS_WGS
// ---
// Note: this parser collects both the Platform and JourneyPlatform resources.
use std::error::Error;
use std::sync::Arc;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::{opt, rest};
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{JourneyId, Version, models::{CoordinateSystem, Coordinates, JourneyPlatform, Model, Platform}, parsing::{
    ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition,
    RowParser,
}, storage::ResourceStorage, utils::{AutoIncrement, create_time_from_value}};

use crate::parsing::ParserFnReturn;

enum RowType {
    RowJourneyPlatform = 1,
    RowPlatform = 2,
    RowSection = 3,
    RowSloid = 4,
    RowCoord = 5,
}

impl TryFrom<i32> for RowType {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RowType::RowJourneyPlatform as i32 => Ok(RowType::RowJourneyPlatform),
            x if x == RowType::RowPlatform as i32 => Ok(RowType::RowPlatform),
            x if x == RowType::RowSection as i32 => Ok(RowType::RowSection),
            x if x == RowType::RowSloid as i32 => Ok(RowType::RowSloid),
            x if x == RowType::RowCoord as i32 => Ok(RowType::RowCoord),

            _ => Err(()),
        }
    }
}

pub struct PlatformParser {
    files: Vec<String>,
    row_parser: Arc<RowParser>
}

impl PlatformParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, take(6usize)),
            preceded(space1, take(6usize)),
            preceded(tag("#"), take(7usize)),
            preceded(space1, opt(take(4usize))),
            preceded(space1, opt(take(6usize)))
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2, data.3, data.4.unwrap_or(""), data.5.unwrap_or("")]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("G"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("A"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_4_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("g A"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_4_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("I A"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_5_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("k"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    fn get_parser_5_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(tag("#"), take(7usize)),
            preceded(tag("K"), preceded(space1, take(6usize))),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    pub fn new(version: Version) -> Self {
        Self {
            // Those are not all files but has not enough time updating my structure to handle name and files
            files: vec!["GLEIS".to_string(), "GLEIS_LV95".to_string(), "GLEIS_WGS".to_string(), "GLEISE_LV95".to_string(), "GLEISE_WGS".to_string()],
            row_parser: Arc::new(RowParser::new({
                let mut rows: Vec<RowDefinition> = vec![];
                rows.push(RowDefinition::new(
                    RowType::RowJourneyPlatform as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer32), // Should be 23-30, but here the # character is ignored.
                        ColumnDefinition::new(ExpectedType::OptionInteger32),
                        ColumnDefinition::new(ExpectedType::OptionInteger32),
                    ],
                    Self::get_parser_1
                ));
                rows.push(RowDefinition::new(
                    RowType::RowPlatform as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32), // Should be 9-16, but here the # character is ignored.
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ));
                if version == Version::V_5_40_41_2_0_7 {
                    // This row is used to give set the Section
                    rows.push(RowDefinition::new(
                        RowType::RowSection as i32,
                        vec![
                            ColumnDefinition::new(ExpectedType::Integer32),
                            ColumnDefinition::new(ExpectedType::Integer32), // Should be 9-16, but here the # character is ignored.
                            ColumnDefinition::new(ExpectedType::String),
                        ],
                        Self::get_parser_3
                    ));
                }
                // This row is used to set the sloid
                rows.push(RowDefinition::new(
                    RowType::RowSloid as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32), // Should be 9-16, but here the # character is ignored.
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    match version {
                        Version::V_5_40_41_2_0_7 => {
                            Self::get_parser_4_1
                        },
                        Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 => {
                            Self::get_parser_4_2
                        }
                    }
                ));
                // This row is used to set the coordinates (either lv95 either wgs84)
                rows.push(RowDefinition::new(
                    RowType::RowCoord as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer32), // Should be 9-16, but here the # character is ignored.
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    match version {
                        Version::V_5_40_41_2_0_7 => {
                            Self::get_parser_5_1
                        },
                        Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 => {
                            Self::get_parser_5_2
                        }
                    }
                ));
                rows
            }))
        }
    }

    pub fn parse(
        &self,
        version: Version,
        path: &str,
        journeys_pk_type_converter: &FxHashSet<JourneyId>,
    ) -> Result<(ResourceStorage<JourneyPlatform>, ResourceStorage<Platform>), Box<dyn Error>> {
        log::info!("Parsing {}...", self.files[0]);
        let auto_increment = AutoIncrement::new();
        let mut platforms = Vec::new();
        let mut platforms_pk_type_converter = FxHashMap::default();

        let mut bytes_offset = 0;
        let mut journey_platform = Vec::new();

        match version {
            Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 => {
                let parser = FileParser::new(&format!("{path}/{}", self.files[0]), Arc::clone(&self.row_parser))?;
                for x in parser.parse() {
                    let (id, bytes_read, values) = x?;
                    match id.try_into() {
                        Ok(RowType::RowJourneyPlatform) => {
                            bytes_offset += bytes_read;
                            journey_platform.push(values);
                        }
                        Ok(RowType::RowPlatform) => {
                            platforms.push(Self::create_instance(
                                values,
                                &auto_increment,
                                &mut platforms_pk_type_converter,
                            )?);
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Version::V_5_40_41_2_0_7 => {
                let parser = FileParser::new(&format!("{path}/{}", self.files[3]), Arc::clone(&self.row_parser))?;
                for x in parser.parse() {
                    let (id, bytes_read, values) = x?;
                    match id.try_into() {
                        Ok(RowType::RowJourneyPlatform)  => {
                            bytes_offset += bytes_read;
                            journey_platform.push(values);
                        }
                        Ok(RowType::RowPlatform)  => {
                            platforms.push(Self::create_instance(
                                values,
                                &auto_increment,
                                &mut platforms_pk_type_converter,
                            )?);
                        }
                        Ok(RowType::RowSection)  => {
                            // We do nothing
                            // We may want to use section at some point
                        }
                        Ok(RowType::RowSloid) | Ok(RowType::RowCoord)  => {
                            // We do nothing, coordinates and sloid are parsed afterwards
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }

        let mut platforms = Platform::vec_to_map(platforms);

        let journey_platform = journey_platform
            .into_iter()
            .map(|values| {
                Self::create_journey_instance(
                    values,
                    journeys_pk_type_converter,
                    &platforms_pk_type_converter,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let journey_platform = JourneyPlatform::vec_to_map(journey_platform);

        log::info!("Parsing {}...", self.files[1]);
        #[rustfmt::skip]
        self.load_coordinates_for_platforms(version, path, CoordinateSystem::LV95, bytes_offset, &platforms_pk_type_converter, &mut platforms)?;
        log::info!("Parsing {}84...", self.files[2]);
        #[rustfmt::skip]
        self.load_coordinates_for_platforms(version, path, CoordinateSystem::WGS84, bytes_offset, &platforms_pk_type_converter, &mut platforms)?;

        Ok((
            ResourceStorage::new(journey_platform),
            ResourceStorage::new(platforms),
        ))
    }

    fn load_coordinates_for_platforms(
        &self,
        version: Version,
        path: &str,
        coordinate_system: CoordinateSystem,
        bytes_offset: u64,
        pk_type_converter: &FxHashMap<(i32, i32), i32>,
        data: &mut FxHashMap<i32, Platform>,
    ) -> Result<(), Box<dyn Error>> {
        let row_parser = self.row_parser.clone();
        let filename = match (version, coordinate_system) {
            (
                Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6,
                CoordinateSystem::LV95,
            ) => self.files[1].clone(),
            (
                Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6,
                CoordinateSystem::WGS84,
            ) => self.files[2].clone(),
            (Version::V_5_40_41_2_0_7, CoordinateSystem::LV95) => self.files[3].clone(),
            (Version::V_5_40_41_2_0_7, CoordinateSystem::WGS84) => self.files[4].clone(),
        };
        let parser =
            FileParser::new_with_bytes_offset(&format!("{path}/{filename}"), row_parser, bytes_offset)?;

        match version {
            Version::V_5_40_41_2_0_4 | Version::V_5_40_41_2_0_5 | Version::V_5_40_41_2_0_6 => {
                parser.parse().try_for_each(|x| {
                    let (id, _, values) = x?;
                    match id.try_into() {
                        Ok(RowType::RowJourneyPlatform)  | Ok(RowType::RowPlatform)  => {
                            // this one has normally already been parsed
                        }
                        Ok(RowType::RowSloid)  => {
                            platform_set_sloid(values, coordinate_system, pk_type_converter, data)?
                        }
                        Ok(RowType::RowCoord)  => platform_set_coordinates(
                            values,
                            coordinate_system,
                            pk_type_converter,
                            data,
                        )?,
                        _ => unreachable!(),
                    }
                    Ok(())
                })
            }
            Version::V_5_40_41_2_0_7 => {
                parser.parse().try_for_each(|x| {
                    let (id, _, values) = x?;
                    match id.try_into() {
                        Ok(RowType::RowJourneyPlatform) | Ok(RowType::RowPlatform) | Ok(RowType::RowSection)  => {
                            // This should already have been treated
                        }
                        Ok(RowType::RowSloid)  => {
                            platform_set_sloid(values, coordinate_system, pk_type_converter, data)?
                        }
                        Ok(RowType::RowCoord)  => platform_set_coordinates(
                            values,
                            coordinate_system,
                            pk_type_converter,
                            data,
                        )?,
                        _ => unreachable!(),
                    }
                    Ok(())
                })
            }
        }
    }

    fn create_instance(
        mut values: Vec<ParsedValue>,
        auto_increment: &AutoIncrement,
        platforms_pk_type_converter: &mut FxHashMap<(i32, i32), i32>,
    ) -> Result<Platform, Box<dyn Error>> {
        let stop_id: i32 = values.remove(0).into();
        let index: i32 = values.remove(0).into();
        let platform_data: String = values.remove(0).into();

        let id = auto_increment.next();
        let (code, sectors) = parse_platform_data(platform_data)?;

        if let Some(previous) = platforms_pk_type_converter.insert((stop_id, index), id) {
            log::warn!(
            "Warning: previous id {previous} for ({stop_id}, {index}). The pair (stop_id, index), ({stop_id}, {index}), is not unique."
        );
        };

        Ok(Platform::new(id, code, sectors, stop_id))
    }

    fn create_journey_instance(
        mut values: Vec<ParsedValue>,
        journeys_pk_type_converter: &FxHashSet<JourneyId>,
        platforms_pk_type_converter: &FxHashMap<(i32, i32), i32>,
    ) -> Result<JourneyPlatform, Box<dyn Error>> {
        let stop_id: i32 = values.remove(0).into();
        let journey_id: i32 = values.remove(0).into();
        let administration: String = values.remove(0).into();
        let index: i32 = values.remove(0).into();
        let time: Option<i32> = values.remove(0).into();
        let bit_field_id: Option<i32> = values.remove(0).into();

        let _journey_id = journeys_pk_type_converter
            .get(&(journey_id, administration.clone()))
            .ok_or("Unknown legacy journey ID")?;

        let platform_id = *platforms_pk_type_converter
            .get(&(stop_id, index))
            .ok_or("Unknown legacy platform ID")?;

        let time = time.map(|x| create_time_from_value(x as u32));

        Ok(JourneyPlatform::new(
            journey_id,
            administration,
            platform_id,
            time,
            bit_field_id,
        ))
    }
}

pub fn parse(
    version: Version,
    path: &str,
    journeys_pk_type_converter: &FxHashSet<JourneyId>,
) -> Result<(ResourceStorage<JourneyPlatform>, ResourceStorage<Platform>), Box<dyn Error>> {
    PlatformParser::new(version).parse(version, path, journeys_pk_type_converter)
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

fn platform_set_sloid(
    mut values: Vec<ParsedValue>,
    coordinate_system: CoordinateSystem,
    pk_type_converter: &FxHashMap<(i32, i32), i32>,
    data: &mut FxHashMap<i32, Platform>,
) -> Result<(), Box<dyn Error>> {
    // The SLOID is processed only when loading LV95 coordinates.
    if coordinate_system == CoordinateSystem::LV95 {
        let stop_id: i32 = values.remove(0).into();
        let index: i32 = values.remove(0).into();
        let sloid: String = values.remove(0).into();

        let id = pk_type_converter
            .get(&(stop_id, index))
            .ok_or("Unknown legacy ID")?;

        data.get_mut(id).ok_or("Unknown ID")?.set_sloid(sloid);
    }

    Ok(())
}

fn platform_set_coordinates(
    mut values: Vec<ParsedValue>,
    coordinate_system: CoordinateSystem,
    pk_type_converter: &FxHashMap<(i32, i32), i32>,
    data: &mut FxHashMap<i32, Platform>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let index: i32 = values.remove(0).into();

    let floats: Vec<_> = String::from(values.remove(0))
        .split_whitespace()
        .map(|v| v.parse::<f64>().unwrap())
        .collect();
    let mut xy1 = floats[0];
    let mut xy2 = floats[1];

    if coordinate_system == CoordinateSystem::WGS84 {
        // WGS84 coordinates are stored in reverse order for some unknown reason.
        (xy1, xy2) = (xy2, xy1);
    }

    let coordinate = Coordinates::new(coordinate_system, xy1, xy2);

    let id = &pk_type_converter
        .get(&(stop_id, index))
        .ok_or("Unknown legacy ID")?;
    let platform = data.get_mut(id).ok_or("Unknown ID")?;

    match coordinate_system {
        CoordinateSystem::LV95 => platform.set_lv95_coordinates(coordinate),
        CoordinateSystem::WGS84 => platform.set_wgs84_coordinates(coordinate),
    }

    Ok(())
}

fn parse_platform_data(
    mut platform_data: String,
) -> Result<(String, Option<String>), Box<dyn Error>> {
    platform_data = format!("{} ", platform_data);
    let data = platform_data.split("' ").filter(|&s| !s.is_empty()).fold(
        FxHashMap::default(),
        |mut acc, item| {
            let parts: Vec<&str> = item.split(" '").collect();
            acc.insert(parts[0], parts[1]);
            acc
        },
    );

    // There should always be a G entry.
    let code = data
        .get("G")
        .ok_or("Entry of type \"G\" missing.")?
        .to_string();
    let sectors = data.get("A").map(|s| s.to_string());

    Ok((code, sectors))
}
