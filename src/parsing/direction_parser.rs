/// # Direction parsing
///
/// This file contains direction informations: namely the Direction ID (that is also contained in
/// the FPLAN file) and the direction Text which gives the last stop of the traject. From
/// [https://opentransportdata.swiss/en/cookbook/hafas-rohdaten-format-hrdf/#Technical_description_What_is_in_the_HRDF_files_contents](HRDF the docs) we have:
///
/// `R000011 Esslingen    % Richtung 11 nach Esslingen`
///
/// that the direction 11 (R is for Richtung) travels to Esslingen
///
/// 1 file(s).
/// File(s) read by the parser:
/// RICHTUNG
use std::error::Error;
use nom::{
    {Parser},
    bytes::complete::take,
    character::complete::space1,
    combinator::rest,
    sequence::preceded,
};
use rustc_hash::FxHashMap;
use crate::{
    models::{Direction, Model},
    parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition, RowParser},
    storage::ResourceStorage,
};
use crate::parsing::{ParserFnReturn};

type DirectionAndTypeConverter = (ResourceStorage<Direction>, FxHashMap<String, i32>);
type FxHashMapsAndTypeConverter = (FxHashMap<i32, Direction>, FxHashMap<String, i32>);

pub struct DirectionParser {
    file: String,
    row_parser: RowParser
}

impl DirectionParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(7usize),
            preceded(space1, rest)
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }
    pub fn new() -> Self {
        Self {
            file: "RICHTUNG".to_string(),
            row_parser: RowParser::new(vec![
                RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::String),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_1
                )
            ])
        }
    }

    fn parse(&self, path: &str) -> Result<DirectionAndTypeConverter, Box<dyn Error>> {
        log::info!("Parsing {}...", self.file);
        let parser = FileParser::new(&format!("{}/{}", path, self.file), self.row_parser.clone())?;
        let (data, pk_type_converter) = row_converter(parser)?;
        Ok((ResourceStorage::new(data), pk_type_converter))
    }
}

pub fn row_converter(
    parser: FileParser,
) -> Result<FxHashMapsAndTypeConverter, Box<dyn Error>> {
    let mut pk_type_converter = FxHashMap::default();

    let data = parser
        .parse()
        .map(|x| x.and_then(|(_, _, values)| create_instance(values, &mut pk_type_converter)))
        .collect::<Result<Vec<_>, _>>()?;
    let data = Direction::vec_to_map(data);
    Ok((data, pk_type_converter))
}

fn create_instance(
    values: Vec<ParsedValue>,
    pk_type_converter: &mut FxHashMap<String, i32>,
) -> Result<Direction, Box<dyn Error>> {
    let (legacy_id, name) = row_from_parsed_values(values);
    let id = remove_first_char(&legacy_id).parse::<i32>()?;

    if let Some(previous) = pk_type_converter.insert(legacy_id.clone(), id) {
        log::warn!(
                "Warning: previous id {previous} for {legacy_id}. The legacy_id, {legacy_id} is not unique."
            );
    }
    Ok(Direction::new(id, name))
}

pub fn parse(path: &str) -> Result<DirectionAndTypeConverter, Box<dyn Error>> {
    DirectionParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn row_from_parsed_values(mut values: Vec<ParsedValue>) -> (String, String) {
    let legacy_id: String = values.remove(0).into();
    let name: String = values.remove(0).into();
    (legacy_id, name)
}

// ------------------------------------------------------------------------------------------------
// --- Helper Functions
// ------------------------------------------------------------------------------------------------

fn remove_first_char(value: &str) -> &str {
    if value.is_empty() {
        return value;
    }

    let mut chars = value.chars();
    chars.next();
    chars.as_str()
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::parsing::tests::get_json_values;
    use pretty_assertions::assert_eq;

    #[test]
    fn row_parser_v207() {
        let rows = vec![
            "R000008 Winterthur".to_string(),
            "R000192 Saas-Fee, Parkhaus".to_string(),
            "R002609 Hégenheim - Collège des Trois Pays".to_string(),
        ];
        let direction_parser = DirectionParser::new();
        let parser = FileParser {
            row_parser: direction_parser.row_parser.clone(),
            rows,
        };
        let mut parser_iterator = parser.parse();
        let (_, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        let (legacy_id, name) = row_from_parsed_values(parsed_values);
        assert_eq!("R000008", &legacy_id);
        assert_eq!("Winterthur", &name);
        let (_, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        let (legacy_id, name) = row_from_parsed_values(parsed_values);
        assert_eq!("R000192", &legacy_id);
        assert_eq!("Saas-Fee, Parkhaus", &name);
        let (_, _, parsed_values) = parser_iterator.next().unwrap().unwrap();
        let (legacy_id, name) = row_from_parsed_values(parsed_values);
        assert_eq!("R002609", &legacy_id);
        assert_eq!("Hégenheim - Collège des Trois Pays", &name);
    }

    #[test]
    fn type_converter_v207() {
        let rows = vec![
            "R000008 Winterthur".to_string(),
            "R000192 Saas-Fee, Parkhaus".to_string(),
            "R002609 Hégenheim - Collège des Trois Pays".to_string(),
        ];
        let direction_parser = DirectionParser::new();
        let parser = FileParser {
            row_parser: direction_parser.row_parser.clone(),
            rows,
        };
        let (data, pk_type_converter) = row_converter(parser).unwrap();
        assert_eq!(*pk_type_converter.get("R000008").unwrap(), 8);
        assert_eq!(*pk_type_converter.get("R000192").unwrap(), 192);
        assert_eq!(*pk_type_converter.get("R002609").unwrap(), 2609);
        let attribute = data.get(&8).unwrap();
        let reference = r#"
            {
                "id":8,
                "name":"Winterthur"
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
        let attribute = data.get(&192).unwrap();
        let reference = r#"
            {
                "id":192,
                "name":"Saas-Fee, Parkhaus"
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
        let attribute = data.get(&2609).unwrap();
        let reference = r#"
            {
                "id":2609,
                "name":"Hégenheim - Collège des Trois Pays"
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
    }
}
