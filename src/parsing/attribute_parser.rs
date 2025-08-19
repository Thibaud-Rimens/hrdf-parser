/// # Attribute parsing
///
/// List of abbreviations describing additional offers (e.g.: dining car)
/// or restrictions (e.g.: seat reservation obligatory). See [https://opentransportdata.swiss/en/cookbook/hafas-rohdaten-format-hrdf/#Technical_description_What_is_in_the_HRDF_files_contents](the documentaion) for more informations.
///
/// This file contains:
///
/// ## The list of offers
///
///
///
/// ### Example (excerpt):
///
/// `
/// Y  0   5  5 % The code Y applies to the journey section (0) with priority 5 and sorting 5
/// `
///
/// ## Description of how the offers can be displayed
///
/// **Important:** Currently these lines are not used in the library
///
/// ### Example (excerpt):
///
/// `
/// # Y  Y  Y  % Attribute code Y should be output as Y for partial route and as Y for full route
/// `
///
/// ## Description in the following languages : German, English, French, Italian
///
/// ## Example (excerpts):
///
/// ...
/// <text>                % Keyword pour la définition du texte
/// <deu>                 % The language becomes german
/// ...
/// Y  Zu Fuss            % Code Y, with description "Zu Fuss"
/// ...
/// <fra>                 % The language becomes French
/// ...
/// Y  A pied             % Code Y, with description "A pied"
/// ...
///
/// File(s) read by the parser:
/// ATTRIBUT
/// ---
/// Files not used by the parser vor version < 2.0.7:
/// ATTRIBUT_DE, ATTRIBUT_EN, ATTRIBUT_FR, ATTRIBUT_IT
/// These files were suppressed in 2.0.7
use std::{error::Error, str::FromStr};
use std::sync::Arc;
use nom::bytes::complete::{tag, take, take_till};
use nom::bytes::is_not;
use nom::character::complete::{char, digit1, space1};
use nom::combinator::{recognize, rest};
use nom::Parser;
use nom::sequence::{delimited, preceded};
use rustc_hash::FxHashMap;

use crate::{models::{Attribute, Language, Model}, parsing::{
    ColumnDefinition, ExpectedType, FileParser,
    ParsedValue, RowDefinition, RowParser,
}, storage::ResourceStorage, utils::AutoIncrement};

use crate::parsing::ParserFnReturn;

type AttributeAndTypeConverter = (ResourceStorage<Attribute>, FxHashMap<String, i32>);
type FxHashMapsAndTypeConverter = (FxHashMap<i32, Attribute>, FxHashMap<String, i32>);

enum RowType {
    RowA = 1,
    RowB = 2,
    RowC = 3,
    RowD = 4,
}

impl TryFrom<i32> for RowType {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RowType::RowA as i32 => Ok(RowType::RowA),
            x if x == RowType::RowB as i32 => Ok(RowType::RowB),
            x if x == RowType::RowC as i32 => Ok(RowType::RowC),
            x if x == RowType::RowD as i32 => Ok(RowType::RowD),
            _ => Err(()),
        }
    }
}

pub struct AttributeParser {
    file: String,
    row_parser: Arc<RowParser>
}

impl AttributeParser {
    fn get_parser_1(input: &'_ str) -> ParserFnReturn<'_> {
        let mut parser = (
            take(2usize),
            preceded(space1, digit1),
            preceded(space1, digit1),
            preceded(space1, digit1)
        );
        let (input, data) = parser.parse(input)?;
        Ok((input, vec![data.0, data.1, data.2, data.3]))
    }
    fn get_parser_2(input: &'_ str) -> ParserFnReturn<'_> {
        let mut parser = recognize((char('#'), rest));
        let (input, data) = parser.parse(input)?;
        Ok((input, vec![data]))
    }
    fn get_parser_3(input: &'_ str) -> ParserFnReturn<'_> {
        let mut parser = recognize((char('<'), rest));
        let (input, data) = parser.parse(input)?;
        Ok((input, vec![data]))
    }
    fn get_parser_4(input: &'_ str) -> ParserFnReturn<'_> {
        let mut parser = (
            take_till(|c| c == ' '),
           (space1, rest)
        );
        let (input, data) = parser.parse(input)?;
        Ok((input, vec![data.0, data.1.1]))
    }
    pub fn new() -> Self {
        Self {
            file: "ATTRIBUT".to_string(),
            row_parser: Arc::new(RowParser::new(vec![
                RowDefinition::new(
                    RowType::RowA as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                        ColumnDefinition::new(ExpectedType::Integer16),
                    ],
                    Self::get_parser_1
                ),
                RowDefinition::new(
                    RowType::RowB as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ),
                RowDefinition::new(
                    RowType::RowC as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_3
                ),
                RowDefinition::new(
                    RowType::RowD as i32,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String)
                    ],
                    Self::get_parser_4
                )
            ]))
        }
    }

    pub fn parse(&self, path: &str) -> Result<AttributeAndTypeConverter, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), Arc::clone(&self.row_parser))?;
        let (data, pk_type_converter) = row_converter(parser)?;
        Ok((ResourceStorage::new(data), pk_type_converter))
    }
}

fn row_converter(
    parser: FileParser,
) -> Result<FxHashMapsAndTypeConverter, Box<dyn Error>> {
    let auto_increment = AutoIncrement::new();
    let mut data = FxHashMap::default();
    let mut pk_type_converter = FxHashMap::default();

    let mut current_language = Language::default();

    for x in parser.parse() {
        let (id, t, values) = x?;
        match id.try_into() {
            Ok(RowType::RowA) => {
                let attribute = create_instance(values, &auto_increment, &mut pk_type_converter);
                data.insert(attribute.id(), attribute);
            },
            Ok(RowType::RowB) => {},
            Ok(RowType::RowC) => update_current_language(values, &mut current_language)?,
            Ok(RowType::RowD) => set_description(values, &pk_type_converter, &mut data, current_language)?,
            _ => unreachable!()

        }
    }
    Ok((data, pk_type_converter))
}

fn create_instance(
    values: Vec<ParsedValue>,
    auto_increment: &AutoIncrement,
    pk_type_converter: &mut FxHashMap<String, i32>,
) -> Attribute {
    let (designation, stop_scope, main_sorting_priority, secondary_sorting_priority) = row_a_from_parsed_values(values);
    let id = auto_increment.next();

    if let Some(previous) = pk_type_converter.insert(designation.to_owned(), id) {
        log::error!(
            "Error: previous id {previous} for {designation}. The designation, {designation}, is not unique."
        );
    }

    Attribute::new(
        id,
        designation.to_owned(),
        stop_scope,
        main_sorting_priority,
        secondary_sorting_priority,
    )
}

pub fn parse(path: &str) -> Result<AttributeAndTypeConverter, Box<dyn Error>> {
    AttributeParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn row_a_from_parsed_values(mut values: Vec<ParsedValue>) -> (String, i16, i16, i16) {
    let designation: String = values.remove(0).into();
    let stop_scope: i16 = values.remove(0).into();
    let main_sorting_priority: i16 = values.remove(0).into();
    let secondary_sorting_priority: i16 = values.remove(0).into();
    (
        designation,
        stop_scope,
        main_sorting_priority,
        secondary_sorting_priority,
    )
}

fn row_c_from_parsed_values(mut values: Vec<ParsedValue>) -> String {
    let language: String = values.remove(0).into();
    language
}

fn row_d_from_parsed_values(mut values: Vec<ParsedValue>) -> (String, String) {
    let legacy_id: String = values.remove(0).into();
    let description: String = values.remove(0).into();
    (legacy_id, description)
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

fn set_description(
    values: Vec<ParsedValue>,
    pk_type_converter: &FxHashMap<String, i32>,
    data: &mut FxHashMap<i32, Attribute>,
    language: Language,
) -> Result<(), Box<dyn Error>> {
    let (legacy_id, description) = row_d_from_parsed_values(values);
    let id = pk_type_converter
        .get(&legacy_id)
        .ok_or("Unknown legacy ID")?;
    data.get_mut(id)
        .ok_or("Unknown ID")?
        .set_description(language, &description);

    Ok(())
}

fn update_current_language(
    values: Vec<ParsedValue>,
    current_language: &mut Language,
) -> Result<(), Box<dyn Error>> {
    let language = row_c_from_parsed_values(values);
    let language = language.replace(['<', '>'], "");

    if language != "text" {
        *current_language = Language::from_str(&language)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::parsing::tests::get_json_values;
    use pretty_assertions::assert_eq;

    #[test]
    fn description_row_d_v206() {
        let rows = vec![
            "VR VELOS: Reservation obligatory".to_string(),
            "2  2nd class only".to_string(),
        ];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: attribute_parser.row_parser.clone(),
            rows: rows.clone(),
        };
        let mut parser_iterator = parser.parse();


        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowD as i32);
        let (legacy_id, description) = row_d_from_parsed_values(parsed_values);
        assert_eq!("VR", &legacy_id);
        assert_eq!("VELOS: Reservation obligatory", &description);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowD as i32);
        let (legacy_id, description) = row_d_from_parsed_values(parsed_values);
        assert_eq!("2", &legacy_id);
        assert_eq!("2nd class only", &description);
    }

    #[test]
    fn parser_row_d_v207() {
        let rows = vec![
            "VR  VELOS: Reservation obligatory".to_string(),
            "2   2nd class only".to_string(),
        ];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: attribute_parser.row_parser.clone(),
            rows: rows.clone(),
        };
        let mut parser_iterator = parser.parse();
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowD as i32);
        let (legacy_id, description) = row_d_from_parsed_values(parsed_values);
        assert_eq!("VR", &legacy_id);
        assert_eq!("VELOS: Reservation obligatory", &description);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowD as i32);
        let (legacy_id, description) = row_d_from_parsed_values(parsed_values);
        assert_eq!("2", &legacy_id);
        assert_eq!("2nd class only", &description);
    }

    #[test]
    fn parser_row_a_v207() {
        let rows = vec!["1  0   1  5".to_string(), "GR 0   6  3".to_string()];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: attribute_parser.row_parser.clone(),
            rows: rows.clone(),
        };
        let mut parser_iterator = parser.parse();
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowA as i32);
        let (designation, stop_scope, main_sorting_priority, secondary_sorting_priority) =
            row_a_from_parsed_values(parsed_values);
        assert_eq!("1", &designation);
        assert_eq!(0, stop_scope);
        assert_eq!(1, main_sorting_priority);
        assert_eq!(5, secondary_sorting_priority);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowA as i32);
        let (designation, stop_scope, main_sorting_priority, secondary_sorting_priority) =
            row_a_from_parsed_values(parsed_values);
        assert_eq!("GR", &designation);
        assert_eq!(0, stop_scope);
        assert_eq!(6, main_sorting_priority);
        assert_eq!(3, secondary_sorting_priority);
    }

    #[test]
    fn type_converter_row_a_v207() {
        let rows = vec![
            "GK 0   4  5".to_string(),
            "<deu>".to_string(),
            "GK  Zollkontrolle möglich, mehr Zeit einrechnen".to_string(),
            "<fra>".to_string(),
            "GK  Contrôle douanier possible, prévoir davantage de temps".to_string(),
            "<ita>".to_string(),
            "GK  Possibile controllo doganale, prevedere più tempo".to_string(),
            "<eng>".to_string(),
            "GK  Possible customs check, please allow extra time".to_string(),
        ];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: Arc::clone(&attribute_parser.row_parser),
            rows: rows.clone(),
        };

        let (data, pk_type_converter) = row_converter(parser).unwrap();
        assert_eq!(*pk_type_converter.get("GK").unwrap(), 1);
        let attribute = data.get(&1).unwrap();
        let reference = r#"
            {
                "id":1,
                "designation":"GK",
                "stop_scope":0,
                "main_sorting_priority":4,
                "secondary_sorting_priority":5,
                "description":{
                    "German":"Zollkontrolle möglich, mehr Zeit einrechnen",
                    "English":"Possible customs check, please allow extra time",
                    "French":"Contrôle douanier possible, prévoir davantage de temps",
                    "Italian":"Possibile controllo doganale, prevedere più tempo"
                }
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
    }

    #[test]
    fn parser_row_b_v207() {
        let rows = vec!["# PG PG PG".to_string()];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: attribute_parser.row_parser.clone(),
            rows: rows.clone(),
        };
        let mut parser_iterator = parser.parse();
        let (id, _, mut parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowB as i32);
        let description: String = parsed_values.remove(0).into();
        assert_eq!(&description, "# PG PG PG");
    }

    #[test]
    fn parser_row_c_v207() {
        let rows = vec![
            "<ita>".to_string(),
            "<fra>".to_string(),
            "<deu>".to_string(),
            "<eng>".to_string(),
            "<text>".to_string(),
        ];
        let attribute_parser = AttributeParser::new();
        let parser = FileParser {
            row_parser: attribute_parser.row_parser.clone(),
            rows: rows.clone(),
        };
        let mut parser_iterator = parser.parse();
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowC as i32);
        let lang = row_c_from_parsed_values(parsed_values);
        assert_eq!(&lang, "<ita>");
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowC as i32);
        let mut current_language = Language::default();
        update_current_language(parsed_values, &mut current_language).unwrap();
        assert_eq!(current_language, Language::French);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowC as i32);
        update_current_language(parsed_values, &mut current_language).unwrap();
        assert_eq!(current_language, Language::German);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowC as i32);
        update_current_language(parsed_values, &mut current_language).unwrap();
        assert_eq!(current_language, Language::English);
        let (id, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        assert_eq!(id, RowType::RowC as i32);
        update_current_language(parsed_values, &mut current_language).unwrap();
        assert_eq!(current_language, Language::English);
    }
}
