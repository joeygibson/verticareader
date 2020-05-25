use std::convert::TryInto;
use std::error::Error;
use std::io::{BufRead, BufReader, Read};
use std::ops::Add;
use std::panic;
use std::result::Result;

use chrono::prelude::*;
use chrono::Duration;
use regex;
use regex::Regex;

use lazy_static::lazy_static;

use crate::column_conversion::ColumnConversion;

#[derive(Debug)]
pub struct ColumnTypes {
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    pub column_conversions: Vec<Option<ColumnConversion>>,
}

impl ColumnTypes {
    pub fn from_reader(reader: impl Read) -> Result<Self, Box<dyn Error>> {
        let mut column_types: Vec<ColumnType> = vec![];
        let mut column_names: Vec<String> = vec![];
        let mut column_conversions: Vec<Option<ColumnConversion>> = vec![];

        let buf = BufReader::new(reader);

        for line in buf
            .lines()
            .filter(|l| l.is_ok() && !l.as_ref().unwrap().is_empty())
        {
            if let Ok(line) = line {
                let chunks: Vec<String> = line.split("/").map(|s| s.to_string()).collect();

                let column_type = ColumnType::from_string(&chunks[0].trim())?;

                let column_name = if chunks.len() > 1 {
                    chunks[1].trim().to_string()
                } else {
                    "".to_string()
                };

                let column_conversion = if chunks.len() > 2 {
                    match ColumnConversion::from_string(chunks[2].trim()) {
                        Ok(column_conversion) => Some(column_conversion),
                        Err(_) => None,
                    }
                } else {
                    None
                };

                column_types.push(column_type);
                column_names.push(column_name);
                column_conversions.push(column_conversion);
            }
        }

        Ok(ColumnTypes {
            column_types,
            column_names,
            column_conversions,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Integer,
    Float,
    Char,
    Varchar,
    Boolean,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Varbinary,
    Binary,
    Numeric,
    Interval,
}

impl ColumnType {
    fn from_string(string: &str) -> Result<ColumnType, String> {
        lazy_static! {
            static ref PAREN_REGEX: Regex = Regex::new(r"\(.+\)").unwrap();
        }

        let no_parens = PAREN_REGEX.replace(string, "");

        let result = match no_parens.to_lowercase().as_str() {
            "integer" | "int" => ColumnType::Integer,
            "float" => ColumnType::Float,
            "char" => ColumnType::Char,
            "varchar" => ColumnType::Varchar,
            "boolean" => ColumnType::Boolean,
            "date" => ColumnType::Date,
            "timestamp" => ColumnType::Timestamp,
            "timestamptz" => ColumnType::TimestampTz,
            "time" => ColumnType::Time,
            "timetz" => ColumnType::TimeTz,
            "varbinary" => ColumnType::Varbinary,
            "binary" => ColumnType::Binary,
            "numeric" => ColumnType::Numeric,
            "interval" => ColumnType::Interval,
            _ => return Err(format!("invalid type: {}", string.clone())),
        };

        Ok(result)
    }

    pub fn format_value(
        &self,
        value: &Option<Vec<u8>>,
        tz_offset: i8,
        column_conversion: &Option<ColumnConversion>,
    ) -> String {
        match &value {
            Some(value) => {
                match &*self {
                    ColumnType::Integer => {
                        let bytes = &value[..];

                        match bytes.len() {
                            8 => format!("{}", u64::from_le_bytes(bytes.try_into().unwrap())),
                            4 => format!("{}", u32::from_le_bytes(bytes.try_into().unwrap())),
                            2 => format!("{}", u16::from_le_bytes(bytes.try_into().unwrap())),
                            1 => format!("{}", u8::from_le_bytes(bytes.try_into().unwrap())),
                            _ => panic!("incorrect integer byte count"),
                        }
                    }
                    ColumnType::Float => {
                        let bytes = &value[..];
                        format!("{}", f64::from_le_bytes(bytes.try_into().unwrap()))
                    }
                    ColumnType::Char | ColumnType::Varchar => {
                        let char_str = match std::str::from_utf8(&value) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("couldn't convert {:X?} to a string: {}", &value, e);
                                "INVALID"
                            }
                        };

                        format!("{}", char_str.trim())
                    }
                    ColumnType::Boolean => format!("{}", value[0] == 1u8),
                    ColumnType::Date => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1);
                        let d = Duration::days(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);
                        format!("{}", new_date)
                    }
                    ColumnType::Timestamp => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);

                        let d = Duration::microseconds(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);
                        format!("{}", new_date)
                    }
                    ColumnType::TimestampTz => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date =
                            NaiveDate::from_ymd(2000, 1, 1).and_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);

                        let tz_offset_date = if tz_offset != 0 {
                            let tz_offset_hours = Duration::hours(tz_offset as i64);
                            new_date.add(tz_offset_hours)
                        } else {
                            new_date
                        };

                        let formatted_tz_offset = format!("{:+03}", tz_offset);
                        let formatted_date =
                            format!("{}{}", tz_offset_date.format("%F %T"), formatted_tz_offset);
                        format!("{}", formatted_date)
                    }
                    ColumnType::Time => {
                        let bytes = &value[..];
                        let microsecond_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;

                        let midnight = NaiveTime::from_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(microsecond_offset);
                        let new_time = midnight.add(d);

                        format!("{}", new_time)
                    }
                    ColumnType::TimeTz => {
                        let bytes = &value[..];
                        let as_u64 = u64::from_le_bytes(bytes.try_into().unwrap());

                        let microsecond_offset: u64 = as_u64 >> 24;
                        let tz_offset_from_column: i64 = (as_u64 & 0xFFFFFF) as i64;

                        let new_offset = -((tz_offset_from_column / 3600) - 24);
                        let midnight = NaiveTime::from_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(microsecond_offset as i64);
                        let new_time = midnight.add(d);

                        // if we leave this as minutes, then we can handle timezones that
                        // don't align to an hour boundary
                        let tz_offset_hours = Duration::hours(new_offset as i64);
                        let offset_time = new_time.add(tz_offset_hours);

                        let formatted_tz_offset = format!("{:+03}", new_offset);
                        let formatted_date =
                            format!("{}{}", offset_time.format("%T"), formatted_tz_offset);
                        format!("{}", formatted_date)
                    }
                    ColumnType::Varbinary | ColumnType::Binary => {
                        let bytes = &value[..];
                        let filtered_bytes = bytes[..]
                            .iter()
                            .filter(|&b| *b != 0x00u8)
                            .map(|b| *b)
                            .collect::<Vec<u8>>();

                        match column_conversion {
                            None => {
                                let byte_values: String =
                                    filtered_bytes.iter()
                                    .map(|b| format!("{:X?}", b))
                                    .collect();

                                format!("0x{}", byte_values)
                            }
                            Some(conversion) => conversion.convert(filtered_bytes),
                        }
                    }
                    ColumnType::Numeric => {
                        // let bytes = &value[..];
                        // let as_u64 = u64::from_le_bytes(bytes.try_into().unwrap());
                        //
                        // println!("VLA: {}", as_u64);
                        "NUMERIC".to_string()
                    }
                    ColumnType::Interval => {
                        let bytes = &value[..];
                        let interval_microseconds = i64::from_le_bytes(bytes.try_into().unwrap());

                        let seconds = interval_microseconds / 1_000_000;
                        let (hours, remainder) = ((seconds / 3600), (seconds % 3600));
                        let (minutes, remainder) = ((remainder / 60), (remainder % 60));

                        format!("{:02}:{:02}:{:02}", hours, minutes, remainder)
                    }
                }
            }
            _ => "".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    mod column_type_tests {
        use crate::column_types::ColumnType;

        #[test]
        fn test_good_input() {
            let exp = ColumnType::Varbinary;
            let val = ColumnType::from_string("Varbinary").unwrap();

            assert_eq!(exp, val);
        }

        #[test]
        fn test_with_mixed_case() {
            let exp = ColumnType::Varbinary;
            let val = ColumnType::from_string("VarBiNarY").unwrap();

            assert_eq!(exp, val);
        }

        #[test]
        fn test_invalid_input() {
            let val = ColumnType::from_string("ljshdflkd");

            assert!(val.is_err(), "should not have returned valid enum");
        }
    }

    mod column_types_tests {
        use crate::column_types::ColumnType::*;
        use crate::column_types::ColumnTypes;

        #[test]
        fn test_good_input() {
            use std::fs::File;

            let file = File::open("data/all-valid-types.txt").unwrap();

            let expected_types = vec![
                Integer,
                Float,
                Char,
                Varchar,
                Boolean,
                Date,
                Timestamp,
                TimestampTz,
                Time,
                TimeTz,
                Varbinary,
                Binary,
                Numeric,
                Interval,
            ];

            let column_types = ColumnTypes::from_reader(file).unwrap();

            assert_eq!(expected_types, column_types.column_types)
        }

        #[test]
        #[should_panic]
        fn test_invalid_input() {
            use std::fs::File;

            let file = File::open("data/types-with-one-invalid.txt").unwrap();

            ColumnTypes::from_reader(file).unwrap();
        }
    }
}
