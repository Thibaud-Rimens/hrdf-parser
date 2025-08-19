// 1 file(s).
// File(s) read by the parser:
// ZUGART
use std::error::Error;
use std::sync::Arc;
use nom::bytes::complete::{tag, take};
use nom::character::complete::space1;
use nom::combinator::{recognize, rest};
use nom::Parser;
use nom::sequence::{pair, preceded};
use rustc_hash::FxHashMap;

use crate::{models::{Language, Model}, parsing::{
    ColumnDefinition, ExpectedType, FileParser,
    ParsedValue, RowDefinition, RowParser,
}, storage::ResourceStorage, utils::AutoIncrement, TransportType, Stop, Attribute};
use crate::parsing::ParserFnReturn;
type TransportTypeAndTypeConverter = (ResourceStorage<TransportType>, FxHashMap<String, i32>);

type FxHashMapsAndTypeConverter = (FxHashMap<i32, TransportType>, FxHashMap<String, i32>);

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

pub struct TransportTypeParser {
    file: String,
    row_parser: Arc<RowParser>
}
impl TransportTypeParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(3usize),
            preceded(space1, take(2usize)),
            preceded(space1, take(1usize)),
            preceded(space1, take(1usize)),
            preceded(space1, take(8usize)),
            preceded(space1, take(1usize)),
            preceded(space1, take(1usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1, data.2, data.3, data.4, data.5, data.6]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = recognize((tag("<"), rest));
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    fn get_parser_3(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded(tag("class"), take(2usize)),
            preceded(space1, rest),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_4(input: &str) -> ParserFnReturn {
        let mut parser = tag("option");
        let (i2, _) = parser.parse(input)?;
        Ok((i2, vec![]))
    }

    fn get_parser_5(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded(tag("category"), take(3usize)),
            preceded(space1, rest),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    fn get_parser_6(input: &str) -> ParserFnReturn {
        let mut parser = (
            preceded(tag("*I"), preceded(space1, take(2usize))),
            take(7usize),
            preceded(space1, take(9usize)),
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new() -> Self {
        Self {
            file: "ZUGART".to_string(),
            row_parser: Arc::new(RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer16),
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
                rows.push(RowDefinition::new(
                    RowType::RowC as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_3
                ));
                rows.push(RowDefinition::new(
                    RowType::RowD as i32,
                    vec![],
                    Self::get_parser_4
                ));
                rows.push(RowDefinition::new(
                    RowType::RowE as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_5
                ));
                rows.push(RowDefinition::new(
                    RowType::RowF as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::OptionInteger32),
                    ],
                    Self::get_parser_6
                ));
                rows
            }))
        }
    }

    fn row_converter(
        &self,
        parser: FileParser,
    ) -> Result<FxHashMapsAndTypeConverter, Box<dyn Error>> {
        let auto_increment = AutoIncrement::new();
        let mut data = Vec::new();
        let mut pk_type_converter = FxHashMap::default();
        let mut current_language = Language::default();

        for x in parser.parse() {
            let (id, _, values) = x?;

            match id.try_into() {
                Ok(RowType::RowA) => data.push(self.create_instance(values, &auto_increment, &mut pk_type_converter)),
                _ => {
                    let transport_type = data.last_mut().ok_or("Type A row missing.")?;

                    match id.try_into() {
                        Ok(RowType::RowB) => update_current_language(values, &mut current_language),
                        Ok(RowType::RowC) => set_product_class_name(values, &mut data, current_language),
                        Ok(RowType::RowD) => {}
                        Ok(RowType::RowE) => set_category_name(values, transport_type, current_language),
                        Ok(RowType::RowF) => {} // TODO: Use information, currently not used
                        _ => unreachable!(),
                    }
                }
            }
        }

        let data = TransportType::vec_to_map(data);
        Ok((data, pk_type_converter))
    }

    fn parse(
        &self,
        path: &str,
    ) -> Result<TransportTypeAndTypeConverter, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{path}/ZUGART"), Arc::clone(&self.row_parser))?;
        let (data, pk_type_converter) = self.row_converter(parser)?;
        Ok((ResourceStorage::new(data), pk_type_converter))
    }

    fn create_instance(
        &self,
        mut values: Vec<ParsedValue>,
        auto_increment: &AutoIncrement,
        pk_type_converter: &mut FxHashMap<String, i32>,
    ) -> TransportType {
        let designation: String = values.remove(0).into();
        let product_class_id: i16 = values.remove(0).into();
        let tarrif_group: String = values.remove(0).into();
        let output_control: i16 = values.remove(0).into();
        let short_name: String = values.remove(0).into();
        let surchage: i16 = values.remove(0).into();
        let flag: String = values.remove(0).into();

        let id = auto_increment.next();

        if let Some(previous) = pk_type_converter.insert(designation.to_owned(), id) {
            log::error!(
            "Warning: previous id {previous} for {designation}. The designation, {designation}, is not unique."
        );
        };
        TransportType::new(
            id,
            designation.to_owned(),
            product_class_id,
            tarrif_group,
            output_control,
            short_name,
            surchage,
            flag,
        )
    }
}


pub fn parse(
    path: &str,
) -> Result<TransportTypeAndTypeConverter, Box<dyn Error>> {
    TransportTypeParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn set_product_class_name(
    mut values: Vec<ParsedValue>,
    data: &mut Vec<TransportType>,
    language: Language,
) {
    let product_class_id: i16 = values.remove(0).into();
    let product_class_name: String = values.remove(0).into();

    for transport_type in data {
        if transport_type.product_class_id() == product_class_id {
            transport_type.set_product_class_name(language, &product_class_name)
        }
    }
}

fn set_category_name(
    mut values: Vec<ParsedValue>,
    transport_type: &mut TransportType,
    language: Language,
) {
    let _: i32 = values.remove(0).into();
    let category_name: String = values.remove(0).into();

    transport_type.set_category_name(language, &category_name);
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

fn update_current_language(mut values: Vec<ParsedValue>, current_language: &mut Language) {
    let language: String = values.remove(0).into();
    let language = &language[1..&language.len() - 1];

    if language != "text" {
        *current_language = match language {
            "Deutsch" => Language::German,
            "Franzoesisch" => Language::French,
            "Englisch" => Language::English,
            "Italienisch" => Language::Italian,
            _ => unreachable!(),
        };
    }
}
