use std::error::Error;
use std::sync::Arc;
use nom::bytes::complete::take;
use nom::character::complete::space1;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;
use crate::parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, ParserFnReturn, RowDefinition, RowParser};
use crate::{Stop};

pub struct ExchangePriorityParser {
    file: String,
    row_parser: Arc<RowParser>
}

impl ExchangePriorityParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, take(2usize)),

        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new() -> Self {
        Self {
            file: "BFPRIOS".to_string(),
            row_parser: Arc::new(RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_1,
                ));
                rows
            }))
        }
    }

    fn parse(
        &self,
        path: &str,
        data: &mut FxHashMap<i32, Stop>,
    ) -> Result<(), Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), Arc::clone(&self.row_parser))?;
        parser.parse().try_for_each(|x| {
            let (_, _, values) = x?;
            set_exchange_priority(values, data)?;
            Ok(())
        })
    }
}

pub fn parse(
    path: &str,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    ExchangePriorityParser::new().parse(path, data)
}

fn set_exchange_priority(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, Stop>,
) -> Result<(), Box<dyn Error>> {
    let stop_id: i32 = values.remove(0).into();
    let exchange_priority: i16 = values.remove(0).into();

    let stop = data.get_mut(&stop_id).ok_or("Unknown ID")?;
    stop.set_exchange_priority(exchange_priority);

    Ok(())
}