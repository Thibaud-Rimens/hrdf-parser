use std::cell::{Ref, RefCell};

use chrono::NaiveDate;

// ------------------------------------------------------------------------------------------------
// --- Coordinate
// ------------------------------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub enum CoordinateType {
    #[default]
    LV95,
    WGS84,
}

#[allow(unused)]
#[derive(Debug, Default)]
pub struct Coordinate {
    // TODO : should I add a getter for the field?
    coordinate_type: CoordinateType,
    x: f64,
    y: f64,
    z: i16,
}

#[allow(unused)]
impl Coordinate {
    pub fn new(coordinate_type: CoordinateType, x: f64, y: f64, z: i16) -> Self {
        Self {
            coordinate_type,
            x,
            y,
            z,
        }
    }

    pub fn easting(&self) -> f64 {
        assert!(self.coordinate_type == CoordinateType::LV95);
        self.x
    }

    pub fn northing(&self) -> f64 {
        assert!(self.coordinate_type == CoordinateType::LV95);
        self.y
    }

    pub fn latitude(&self) -> f64 {
        assert!(self.coordinate_type == CoordinateType::WGS84);
        self.x
    }

    pub fn longitude(&self) -> f64 {
        assert!(self.coordinate_type == CoordinateType::WGS84);
        self.y
    }

    pub fn altitude(&self) -> i16 {
        self.z
    }
}

// ------------------------------------------------------------------------------------------------
// --- JourneyPlatform
// ------------------------------------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug)]
pub struct JourneyPlatform {
    journey_id: i32,
    platform_id: i64, // Haltestellennummer << 32 + "Index der Gleistextinformation"
    unknown1: String, // "Verwaltung für Fahrt"
    hour: Option<i16>,
    bit_field_id: Option<i32>,
}

#[allow(unused)]
impl JourneyPlatform {
    pub fn new(
        journey_id: i32,
        platform_id: i64,
        unknown1: String,
        hour: Option<i16>,
        bit_field_id: Option<i32>,
    ) -> Self {
        Self {
            journey_id,
            platform_id,
            unknown1,
            hour,
            bit_field_id,
        }
    }

    pub fn journey_id(&self) -> i32 {
        self.journey_id
    }

    pub fn platform_id(&self) -> i64 {
        self.platform_id
    }

    pub fn unknown1(&self) -> &String {
        &self.unknown1
    }

    pub fn hour(&self) -> &Option<i16> {
        &self.hour
    }

    pub fn bit_field_id(&self) -> &Option<i32> {
        &self.bit_field_id
    }
}

// ------------------------------------------------------------------------------------------------
// --- Platform
// ------------------------------------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug, Default)]
pub struct Platform {
    id: i64, // Haltestellennummer << 32 + "Index der Gleistextinformation"
    number: String,
    sectors: Option<String>,
    sloid: RefCell<String>,
    lv95_coordinate: RefCell<Coordinate>,
    wgs84_coordinate: RefCell<Coordinate>,
}

#[allow(unused)]
impl Platform {
    pub fn new(id: i64, number: String, sectors: Option<String>) -> Self {
        Self {
            id,
            number,
            sectors,
            sloid: RefCell::new(String::default()),
            lv95_coordinate: RefCell::new(Coordinate::default()),
            wgs84_coordinate: RefCell::new(Coordinate::default()),
        }
    }

    pub fn create_id(stop_id: i32, pindex: i32) -> i64 {
        ((stop_id as i64) << 32) + (pindex as i64)
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn number(&self) -> &String {
        &self.number
    }

    pub fn sectors(&self) -> &Option<String> {
        &self.sectors
    }

    pub fn sloid(&self) -> Ref<'_, String> {
        self.sloid.borrow()
    }

    pub fn set_sloid(&self, sloid: String) {
        *self.sloid.borrow_mut() = sloid;
    }

    pub fn lv95_coordinate(&self) -> Ref<'_, Coordinate> {
        self.lv95_coordinate.borrow()
    }

    pub fn set_lv95_coordinate(&self, coordinate: Coordinate) {
        *self.lv95_coordinate.borrow_mut() = coordinate;
    }

    pub fn wgs84_coordinate(&self) -> Ref<'_, Coordinate> {
        self.wgs84_coordinate.borrow()
    }

    pub fn set_wgs84_coordinate(&self, coordinate: Coordinate) {
        *self.wgs84_coordinate.borrow_mut() = coordinate;
    }
}

// ------------------------------------------------------------------------------------------------
// --- Stop
// ------------------------------------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug)]
pub struct Stop {
    id: i32,
    name: String,
    long_name: Option<String>,
    abbreviation: Option<String>,
    synonyms: Option<Vec<String>>,
    lv95_coordinate: RefCell<Option<Coordinate>>,
    wgs84_coordinate: RefCell<Option<Coordinate>>,
}

#[allow(unused)]
impl Stop {
    pub fn new(
        id: i32,
        name: String,
        long_name: Option<String>,
        abbreviation: Option<String>,
        synonyms: Option<Vec<String>>,
    ) -> Self {
        Self {
            id,
            name,
            long_name,
            abbreviation,
            synonyms,
            lv95_coordinate: RefCell::new(None),
            wgs84_coordinate: RefCell::new(None),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn long_name(&self) -> &Option<String> {
        &self.long_name
    }

    pub fn abbreviation(&self) -> &Option<String> {
        &self.abbreviation
    }

    pub fn synonyms(&self) -> &Option<Vec<String>> {
        &self.synonyms
    }

    pub fn lv95_coordinate(&self) -> Ref<'_, Option<Coordinate>> {
        self.lv95_coordinate.borrow()
    }

    pub fn set_lv95_coordinate(&self, coordinate: Coordinate) {
        *self.lv95_coordinate.borrow_mut() = Some(coordinate);
    }

    pub fn wgs84_coordinate(&self) -> Ref<'_, Option<Coordinate>> {
        self.wgs84_coordinate.borrow()
    }

    pub fn set_wgs84_coordinate(&self, coordinate: Coordinate) {
        *self.wgs84_coordinate.borrow_mut() = Some(coordinate);
    }
}

// ------------------------------------------------------------------------------------------------
// --- TimetableKeyData
// ------------------------------------------------------------------------------------------------

#[allow(unused)]
#[derive(Debug)]
pub struct TimetableKeyData {
    start_date: NaiveDate, // The date is included.
    end_date: NaiveDate,   // The date is included.
    metadata: Vec<String>,
}

#[allow(unused)]
impl TimetableKeyData {
    pub fn new(start_date: NaiveDate, end_date: NaiveDate, metadata: Vec<String>) -> Self {
        Self {
            start_date,
            end_date,
            metadata,
        }
    }

    pub fn start_date(&self) -> &NaiveDate {
        &self.start_date
    }

    pub fn end_date(&self) -> &NaiveDate {
        &self.end_date
    }

    pub fn metadata(&self) -> &Vec<String> {
        &self.metadata
    }
}
