use std::error::Error;
use nom::bytes::complete::take;
use nom::character::complete::space1;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;
use crate::parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, ParserFnReturn, RowDefinition, RowParser};
use crate::{Stop};

pub struct ExchangeTimeParser {
    file: String,
    row_parser: RowParser
}

impl ExchangeTimeParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, take(2usize)),
            preceded(space1, take(2usize))
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2]))
    }

    pub fn new() -> Self {
        Self {
            file: "UMSTEIGB".to_string(),
            row_parser: RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_1,
                ));
                rows
            })
        }
    }

    fn parse(
        &self,
        path: &str,
        data: &mut FxHashMap<i32, Stop>,
    ) -> Result<(i16, i16), Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let mut default_exchange_time = (0, 0);
        parser.parse().try_for_each(|x| {
            let (_, _, values) = x?;
            if let Some(x) = set_exchange_time(values, data)? {
                default_exchange_time = x;
            }
            Ok::<(), Box<dyn Error>>(())
        })?;

        Ok(default_exchange_time)
    }
}

pub(crate) fn parse(path: &str, data: &mut FxHashMap<i32, Stop>, ) -> Result<(i16, i16), Box<dyn Error>> {
    ExchangeTimeParser::new().parse(path, data)
}

fn set_exchange_time(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<Option<(i16, i16)>, Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let exchange_time_inter_city: i16 = values.remove(0).into();
    let exchange_time_other: i16 = values.remove(0).into();

    let exchange_time = Some((exchange_time_inter_city, exchange_time_other));

    if stop_id == 9999999 {
        // The first row of the file has the stop ID number 9999999.
        // It contains default exchange times to be used when a stop has no specific exchange time.
        Ok(exchange_time)
    } else {
        let stop = data.get_mut(&stop_id).ok_or("Unknown ID")?;
        stop.set_exchange_time(exchange_time);
        Ok(None)
    }
}