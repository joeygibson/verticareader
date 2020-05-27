use std::error::Error;
use std::io::{BufRead, BufReader, Read};
use std::result::Result;

use crate::column_conversion::ColumnConversion;
use crate::column_type::ColumnType;

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

#[cfg(test)]
mod tests {
    use crate::column_type::ColumnType::*;
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
