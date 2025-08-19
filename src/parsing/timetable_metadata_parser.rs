// 1 file(s).
// File(s) read by the parser:
// ECKDATEN
use std::error::Error;
use chrono::NaiveDate;
use nom::bytes::complete::take;
use nom::character::complete::char;
use nom::combinator::{recognize, rest};
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::{FxHashMap};
use crate::{models::{Model, TimetableMetadataEntry}, parsing::{
    ColumnDefinition, ExpectedType, FileParser,
    ParsedValue, RowDefinition, RowParser,
}, storage::ResourceStorage, utils::AutoIncrement};
use crate::parsing::ParserFnReturn;

enum RowType {
    RowA = 1,
    RowB = 2,
}

impl TryFrom<i32> for RowType {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RowType::RowA as i32 => Ok(RowType::RowA),
            x if x == RowType::RowB as i32 => Ok(RowType::RowB),
            _ => Err(()),
        }
    }
}

pub struct TimetableMetadataParser {
    file: String,
    row_parser: RowParser
}

impl TimetableMetadataParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = recognize((
            take(2usize),
            preceded(char('.'), take(2usize)),
            preceded(char('.'), take(4usize))
        ));
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = rest;
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    pub fn new() -> Self {
        Self {
            file: "ECKDATEN".to_string(),
            row_parser: RowParser::new( {
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_1
                ));
                rows.push(RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ));
                rows
            })
        }
    }

    fn parse(&self, path: &str, ) -> Result<ResourceStorage<TimetableMetadataEntry>, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let data = row_converter(parser)?;
        Ok(ResourceStorage::new(data))
    }
}

fn row_converter(parser: FileParser) -> Result<FxHashMap<i32, TimetableMetadataEntry>, Box<dyn Error>>{
    let auto_increment = AutoIncrement::new();
    let data: Vec<ParsedValue> = parser
        .parse()
        .map(|x| x.map(|(_, _, mut values)| values.remove(0)))
        .collect::<Result<Vec<_>, _>>()?;

    let data = create_instance(data, &auto_increment);
    let data = TimetableMetadataEntry::vec_to_map(data?);
    Ok(data)
}

fn create_instance(
    mut values: Vec<ParsedValue>,
    auto_increment: &AutoIncrement,
) -> Result<Vec<TimetableMetadataEntry>, Box<dyn Error>> {
    let start_date: String = values.remove(0).into();
    let end_date: String = values.remove(0).into();
    let other_data: String = values.remove(0).into();

    let start_date = NaiveDate::parse_from_str(&start_date, "%d.%m.%Y")?;
    let end_date = NaiveDate::parse_from_str(&end_date, "%d.%m.%Y")?;
    let other_data: Vec<String> = other_data.split('$').map(String::from).collect();

    let rows = vec![
        ("start_date", start_date.to_string()),
        ("end_date", end_date.to_string()),
        ("name", other_data[0].to_owned()),
        ("created_at", other_data[1].to_owned()),
        ("version", other_data[2].to_owned()),
        ("provider", other_data[3].to_owned()),
    ];

    let data: Vec<TimetableMetadataEntry> = rows.iter()
        .map(|(key, value)| {
            TimetableMetadataEntry::new(auto_increment.next(), key.to_string(), value.to_owned())
        })
        .collect();

    Ok(data)
}

pub fn parse(path: &str) -> Result<ResourceStorage<TimetableMetadataEntry>, Box<dyn Error>> {
    TimetableMetadataParser::new().parse(path)
}
