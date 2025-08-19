/// # Infotext parsing
///
/// Additional information on objects (journeys, lines, etc.). This information can either be
///
/// - Be simple texts, e.g.: 000018154 Rollstühle können mit Unterstützung des Fahrpersonals befördert werden,
/// - Values with semantic meaning. This means values that cannot be represented in any other way and have therefore been “outsourced” to INFOTEXT, e.g.  000000000 ch:1:sjyid:100001:3-002
///
/// The INFOTEXTCODE attribute defines whether these are simple texts or texts with a semantic meaning.
/// The INFOTEXTCODE is not in the INFOTEXT file, but only in the INFOTEXT referencing files, e.g. FPLAN.
///
/// ## Remark
///
/// We start by parsing the INFOTEXT_DE file to get the ids of each ilne and then complement them
/// with the rest of the infotext from INFOTEXT_* for the semantic meaning part, since all
/// files have the same content from this point of view. The rest is parsed by language
///
/// 4 file(s).
/// File(s) read by the parser:
/// INFOTEXT_DE, INFOTEXT_EN, INFOTEXT_FR, INFOTEXT_IT
use std::error::Error;
use nom::bytes::complete::take;
use nom::character::complete::space1;
use nom::combinator::rest;
use nom::Parser;
use nom::sequence::preceded;
use rustc_hash::FxHashMap;

use crate::{
    models::{InformationText, Language, Model},
    parsing::{ColumnDefinition, ExpectedType, FileParser, ParsedValue, RowDefinition, RowParser},
    storage::ResourceStorage,
};
use crate::parsing::ParserFnReturn;

pub struct InformationTextParser {
    files: Vec<String>,
    languages: Vec<Language>,
    id_row_parser: RowParser,
    infotext_row_parser: RowParser,
}

impl InformationTextParser {
    fn get_parser_1(input: &str) -> ParserFnReturn {
        let mut parser = take(9usize);
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data]))
    }

    fn get_parser_2(input: &str) -> ParserFnReturn {
        let mut parser = (
            take(9usize),
            preceded(space1, rest)
        );
        let (i2, data) = parser.parse(input)?;
        Ok((i2, vec![data.0, data.1]))
    }

    pub fn new() -> Self {
        Self {
            files: vec!["INFOTEXT_DE".to_string(), "INFOTEXT_EN".to_string(), "INFOTEXT_FR".to_string(), "INFOTEXT_IT".to_string()],
            languages: vec![Language::German, Language::English, Language::French, Language::Italian],
            id_row_parser: RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32)
                    ],
                    Self::get_parser_1
                ));
                rows
            }),
            infotext_row_parser: RowParser::new({
                let mut rows = vec![];
                rows.push(RowDefinition::new(
                    0,
                    vec![
                        ColumnDefinition::new(ExpectedType::Integer32),
                        ColumnDefinition::new(ExpectedType::String),
                    ],
                    Self::get_parser_2
                ));
                rows
            })
        }
    }

    pub fn parse(&self, path: &str) -> Result<ResourceStorage<InformationText>, Box<dyn Error>> {
        for file in self.files.iter() {
            log::info!("Parsing {}...", file);
        }

        let parser = FileParser::new(&format!("{}/{}", path, self.files[0]), self.id_row_parser.clone())?;
        let mut data = id_row_converter(parser)?;

        for language in self.languages.iter() {
            self.parse_infotext(path, &mut data, *language)?;
        }

        Ok(ResourceStorage::new(data))
    }

    fn parse_infotext(
        &self,
        path: &str,
        data: &mut FxHashMap<i32, InformationText>,
        language: Language,
    ) -> Result<(), Box<dyn Error>> {
        let filename = match language {
            Language::German => "INFOTEXT_DE",
            Language::English => "INFOTEXT_EN",
            Language::French => "INFOTEXT_FR",
            Language::Italian => "INFOTEXT_IT",
        };
        let parser = FileParser::new(&format!("{path}/{filename}"), self.infotext_row_parser.clone())?;
        infotext_row_converter(parser, data, language)
    }
}

fn id_row_converter(parser: FileParser) -> Result<FxHashMap<i32, InformationText>, Box<dyn Error>> {
    let data = parser
        .parse()
        .map(|x| x.map(|(_, _, values)| create_instance(values)))
        .collect::<Result<Vec<_>, _>>()?;
    let data = InformationText::vec_to_map(data);
    Ok(data)
}

fn infotext_row_converter(
    parser: FileParser,
    data: &mut FxHashMap<i32, InformationText>,
    language: Language,
) -> Result<(), Box<dyn Error>> {
    parser.parse().try_for_each(|x| {
        let (_, _, values) = x?;
        set_content(values, data, language)?;
        Ok(())
    })
}

pub fn parse(path: &str) -> Result<ResourceStorage<InformationText>, Box<dyn Error>> {
    InformationTextParser::new().parse(path)
}

// ------------------------------------------------------------------------------------------------
// --- Data Processing Functions
// ------------------------------------------------------------------------------------------------

fn create_instance(mut values: Vec<ParsedValue>) -> InformationText {
    let id: i32 = values.remove(0).into();

    InformationText::new(id)
}

fn set_content(
    mut values: Vec<ParsedValue>,
    data: &mut FxHashMap<i32, InformationText>,
    language: Language,
) -> Result<(), Box<dyn Error>> {
    let id: i32 = values.remove(0).into();
    let description: String = values.remove(0).into();

    data.get_mut(&id)
        .ok_or("Unknown ID")?
        .set_content(language, &description);

    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::parsing::tests::get_json_values;
    use pretty_assertions::assert_eq;

    #[test]
    fn id_row_parser_v207() {
        let rows = vec![
            "000001921 ch:1:sjyid:100001:3995-001".to_string(),
            "000003459 2518".to_string(),
        ];
        let information_text_parser = InformationTextParser::new();
        let parser = FileParser {
            row_parser: information_text_parser.id_row_parser.clone(),
            rows,
        };
        let mut parser_iterator = parser.parse();
        let (_, _, mut parsed_values) = parser_iterator.next().unwrap().unwrap();
        let id: i32 = parsed_values.remove(0).into();
        assert_eq!(1921, id);
        let (_, _, mut parsed_values) = parser_iterator.next().unwrap().unwrap();
        let id: i32 = parsed_values.remove(0).into();
        assert_eq!(3459, id);
    }

    #[test]
    fn id_type_converter_v207() {
        let rows = vec![
            "000001921 ch:1:sjyid:100001:3995-001".to_string(),
            "000003459 2518".to_string(),
        ];
        let information_text_parser = InformationTextParser::new();
        let parser = FileParser {
            row_parser: information_text_parser.id_row_parser.clone(),
            rows,
        };
        let data = id_row_converter(parser).unwrap();
        // First row (id: 1)
        let attribute = data.get(&1921).unwrap();
        let reference = r#"
            {
                "id": 1921,
                "content": {}
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
        // Second row (id: 2)
        let attribute = data.get(&3459).unwrap();
        let reference = r#"
            {
                "id": 3459,
                "content": {}
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
    }

    #[test]
    fn infotext_row_parser_v207() {
        let rows = vec![
            "000001921 ch:1:sjyid:100001:3995-001".to_string(),
            "000003459 2518".to_string(),
        ];
        let information_text_parser = InformationTextParser::new();
        let parser = FileParser {
            row_parser: information_text_parser.infotext_row_parser.clone(),
            rows,
        };
        let mut parser_iterator = parser.parse();
        let (_, _, mut parsed_values) = parser_iterator.next().unwrap().unwrap();
        let id: i32 = parsed_values.remove(0).into();
        assert_eq!(1921, id);
        let content: String = parsed_values.remove(0).into();
        assert_eq!("ch:1:sjyid:100001:3995-001", &content);
        let (_, _, mut parsed_values) = parser_iterator.next().unwrap().unwrap();
        let id: i32 = parsed_values.remove(0).into();
        assert_eq!(3459, id);
        let content: String = parsed_values.remove(0).into();
        assert_eq!("2518", &content);
    }

    #[test]
    fn infotext_type_converter_v207() {
        let rows = vec![
            "000001921 ch:1:sjyid:100001:3995-001".to_string(),
            "000003459 2518".to_string(),
        ];
        let information_text_parser = InformationTextParser::new();
        let parser = FileParser {
            row_parser: information_text_parser.infotext_row_parser.clone(),
            rows: rows.clone(),
        };
        let mut data = id_row_converter(parser).unwrap();

        for language in information_text_parser.languages.iter() {
            let parser = FileParser {
                row_parser: information_text_parser.infotext_row_parser.clone(),
                rows: rows.clone(),
            };

            infotext_row_converter(parser, &mut data, *language).unwrap();
        }

        // First row (id: 1)
        let attribute = data.get(&1921).unwrap();
        let reference = r#"
            {
                "id": 1921,
                "content": {
                    "French": "ch:1:sjyid:100001:3995-001",
                    "Italian": "ch:1:sjyid:100001:3995-001",
                    "German": "ch:1:sjyid:100001:3995-001",
                    "English": "ch:1:sjyid:100001:3995-001"
                }
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
        // Second row (id: 2)
        let attribute = data.get(&3459).unwrap();
        let reference = r#"
            {
                "id": 3459,
                "content": {
                    "French": "2518",
                    "Italian": "2518",
                    "German": "2518",
                    "English": "2518"
                }
            }"#;
        let (attribute, reference) = get_json_values(attribute, reference).unwrap();
        assert_eq!(attribute, reference);
    }
}