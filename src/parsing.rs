mod attribute_parser;
mod bit_field_parser;
mod direction_parser;
mod exchange_administration_parser;
mod exchange_journey_parser;
mod exchange_line_parser;
mod holiday_parser;
mod information_text_parser;
mod journey_parser;
mod line_parser;
mod platform_parser;
mod stop_connection_parser;
mod stop_parser;
mod through_service_parser;
mod timetable_metadata_parser;
mod transport_company_parser;
mod transport_type_parser;
mod coordinate_parser;
mod exchange_priority_parser;
mod exchange_flag_parser;
mod exchange_time_parser;
mod description_parser;

pub use attribute_parser::parse as load_attributes;
pub use bit_field_parser::parse as load_bit_fields;
pub use direction_parser::parse as load_directions;
pub use exchange_administration_parser::parse as load_exchange_times_administration;
pub use exchange_journey_parser::parse as load_exchange_times_journey;
pub use exchange_line_parser::parse as load_exchange_times_line;
pub use holiday_parser::parse as load_holidays;
pub use information_text_parser::parse as load_information_texts;
pub use journey_parser::parse as load_journeys;
pub use line_parser::parse as load_lines;
pub use platform_parser::parse as load_platforms;
pub use stop_connection_parser::parse as load_stop_connections;
pub use stop_parser::parse as load_stops;
pub use through_service_parser::parse as load_through_service;
pub use timetable_metadata_parser::parse as load_timetable_metadata;
pub use transport_company_parser::parse as load_transport_companies;
pub use transport_type_parser::parse as load_transport_types;

use std::{
    error::Error,
    fs::File,
    io::{self, Read, Seek},
};

use nom::{
    IResult,
};

use serde_json::{Number, Value};

#[derive(Clone, Debug)]
pub enum ExpectedType {
    Float,
    Integer16,
    Integer32,
    String,
    OptionInteger32,
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum ParsedValue {
    Float(f64),
    Integer16(i16),
    Integer32(i32),
    String(String),
    OptionInteger16(Option<i16>),
    OptionInteger32(Option<i32>),
}

impl From<ParsedValue> for f64 {
    fn from(value: ParsedValue) -> Self {
        match value {
            ParsedValue::Float(x) => x,
            // If this error occurs, it's due to a typing error and it's the developer's fault.
            _ => panic!("Failed to convert ParsedValue to f64."),
        }
    }
}

impl From<ParsedValue> for i16 {
    fn from(value: ParsedValue) -> Self {
        match value {
            ParsedValue::Integer16(x) => x,
            // If this error occurs, it's due to a typing error and it's the developer's fault.
            _ => panic!("Failed to convert ParsedValue to i16."),
        }
    }
}

impl From<ParsedValue> for i32 {
    fn from(value: ParsedValue) -> Self {
        match value {
            ParsedValue::Integer32(x) => x,
            // If this error occurs, it's due to a typing error and it's the developer's fault.
            _ => panic!("Failed to convert ParsedValue to i32."),
        }
    }
}

impl From<ParsedValue> for String {
    fn from(value: ParsedValue) -> Self {
        match value {
            ParsedValue::String(x) => x,
            // If this error occurs, it's due to a typing error and it's the developer's fault.
            _ => panic!("Failed to convert ParsedValue to String."),
        }
    }
}

impl From<ParsedValue> for Option<i32> {
    fn from(value: ParsedValue) -> Self {
        match value {
            ParsedValue::OptionInteger32(x) => x,
            // If this error occurs, it's due to a typing error and it's the developer's fault.
            _ => panic!("Failed to convert ParsedValue to Option<i32>."),
        }
    }
}

impl From<ParsedValue> for Value {
    #[track_caller]
    fn from(value: ParsedValue) -> Value {
        match value {
            ParsedValue::Float(x) => {
                Number::from_f64(x)
                    .map(Value::Number)
                    .unwrap_or_else(|| panic!("Non-finite float (NaN/±Inf) not allowed in JSON"))
            }
            ParsedValue::Integer16(x)       => Value::from(x),
            ParsedValue::Integer32(x)       => Value::from(x),
            ParsedValue::String(s)          => Value::from(s),
            ParsedValue::OptionInteger32(o) => match o {
                Some(n) => Value::from(n),
                None    => Value::Null,
            },
            ParsedValue::OptionInteger16(o) => match o {
                Some(n) => Value::from(n),
                None    => Value::Null,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    expected: ExpectedType,
}

impl ColumnDefinition {
    pub fn new (expected: ExpectedType) -> Self {
        Self { expected }
    }
}

pub type ParserFnReturn<'a> = IResult<&'a str, Vec<&'a str>>;
pub type ParserFn = for<'a> fn(&'a str) -> ParserFnReturn<'a>;

#[derive(Clone, Debug)]
pub struct RowDefinition {
    pub id: i32,
    pub definition: Vec<ColumnDefinition>,
    pub parser: ParserFn
}

impl RowDefinition {
    pub fn new (id: i32, definition: Vec<ColumnDefinition>, parser: ParserFn) -> Self {
        Self { id, definition, parser }
    }
}

#[derive(Clone, Debug)]
pub struct RowParser {
    row_definitions: Vec<RowDefinition>,
}

impl RowParser {
    pub fn new(row_definitions: Vec<RowDefinition>) -> Self {
        Self { row_definitions }
    }

    fn parse(&self, row: &str) -> Result<ParsedRow, Box<dyn Error>> {
        let mut parsed_row: Option<ParsedRow> = None;
        for row_definition in self.row_definitions.iter() {
            if let Ok((_, values)) = (row_definition.parser)(row) {
                let values: Vec<&str> = values.iter().map(|x| x.trim()).collect();
                let mut parsed_values: Vec<ParsedValue> = Vec::with_capacity(values.len());
                for (value, col_def) in values.iter().zip(row_definition.definition.iter()) {
                    let parsed_value = match col_def.expected {
                        ExpectedType::Float => ParsedValue::Float(value.parse()?),
                        ExpectedType::Integer16 => ParsedValue::Integer16(value.parse()?),
                        ExpectedType::Integer32 => ParsedValue::Integer32(value.parse()?),
                        ExpectedType::String => ParsedValue::String(value.to_string()),
                        ExpectedType::OptionInteger32 => ParsedValue::OptionInteger32(value.parse().ok()),
                    };
                    parsed_values.push(parsed_value);
                }

                // 2 bytes for \r\n
                let bytes_read = row.len() as u64 + 2;
                parsed_row = Some((row_definition.id, bytes_read, parsed_values));
                break;
            }
        }

        parsed_row.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "RowType not found!")
        }).map_err(|e| e.into())
    }
}

// (RowDefinition.id, number of bytes read, values parsed from the row)
type ParsedRow = (i32, u64, Vec<ParsedValue>);

pub struct FileParser {
    rows: Vec<String>,
    row_parser: RowParser,
}

impl FileParser {
    pub fn new(path: &str, row_parser: RowParser) -> io::Result<Self> {
        Self::new_with_bytes_offset(path, row_parser, 0)
    }

    pub fn new_with_bytes_offset(
        path: &str,
        row_parser: RowParser,
        bytes_offset: u64,
    ) -> io::Result<Self> {
        let rows = Self::read_lines(path, bytes_offset)?;
        Ok(Self { rows, row_parser })
    }

    fn read_lines(path: &str, bytes_offset: u64) -> io::Result<Vec<String>> {
        let mut file = File::open(path)?;
        file.seek(io::SeekFrom::Start(bytes_offset))?;
        let mut reader = io::BufReader::new(file);
        let mut contents = String::new();
        reader.read_to_string(&mut contents)?;
        let lines = contents.lines().map(String::from).collect();
        Ok(lines)
    }

    pub fn parse(&'_ self) -> ParsedRowIterator<'_> {
        ParsedRowIterator {
            rows_iter: self.rows.iter(),
            row_parser: &self.row_parser,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// --- ParsedRowIterator
// ------------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParsedRowIterator<'a> {
    rows_iter: std::slice::Iter<'a, String>,
    row_parser: &'a RowParser,
}

impl Iterator for ParsedRowIterator<'_> {
    type Item = Result<ParsedRow, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows_iter
            .by_ref()
            .find(|row| !row.trim().is_empty())
            .map(|row| self.row_parser.parse(row))
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use serde::{Deserialize, Serialize};

    pub(crate) fn get_json_values<F>(
        lhs: &F,
        rhs: &str,
    ) -> Result<(serde_json::Value, serde_json::Value), Box<dyn Error>>
    where
        for<'a> F: Serialize + Deserialize<'a>,
    {
        let serialized = serde_json::to_string(&lhs)?;
        let reference = serde_json::to_string(&serde_json::from_str::<F>(rhs)?)?;
        Ok((
            serialized.parse::<serde_json::Value>()?,
            reference.parse::<serde_json::Value>()?,
        ))
    }
}
