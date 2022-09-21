use std::collections::HashMap;
use std::error::Error;
use std::io::Read;

use serde_json::{Number, Value};

use crate::column_definitions::ColumnDefinitions;
use crate::column_type::ColumnType;
use crate::column_types::ColumnTypes;
use crate::file_signature::FileSignature;
use crate::{convert_to_float, convert_to_int, read_u32, read_u8, read_variable};

pub struct VerticaNativeFile<'a> {
    _signature: FileSignature,
    pub definitions: ColumnDefinitions,
    file: &'a mut dyn Read,
}

impl<'a> VerticaNativeFile<'a> {
    pub fn from_reader(reader: &'a mut impl Read) -> Result<Self, Box<dyn Error>> {
        let signature = FileSignature::from_reader(reader)?;
        let definitions = ColumnDefinitions::from_reader(reader)?;

        Ok(VerticaNativeFile {
            _signature: signature,
            definitions,
            file: reader,
        })
    }
}

impl<'a> Iterator for VerticaNativeFile<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row_length = match read_u32(&mut self.file) {
            Ok(length) => length,
            Err(_) => return None,
        };

        if row_length <= 0 {
            return None;
        }

        match Row::from_reader(&mut self.file, &self.definitions.column_widths) {
            Ok(row) => Some(row),
            Err(e) => {
                eprintln!("reading data: {}", e);
                None
            }
        }
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct Row {
    pub null_values: Vec<bool>,
    pub data: Vec<Option<Vec<u8>>>,
}

impl Row {
    fn from_reader(
        reader: &mut impl Read,
        column_widths: &Vec<u32>,
    ) -> Result<Self, Box<dyn Error>> {
        let mut data: Vec<Option<Vec<u8>>> = vec![];

        let null_values = Row::read_bitfield(reader, &column_widths)?;

        for (index, width) in column_widths.iter().enumerate() {
            if null_values[index] {
                data.push(None);
                continue;
            }

            let mut column: Vec<u8> = vec![];

            let column_width = if *width == u32::MAX {
                read_u32(reader)?
            } else {
                *width
            };

            for _ in 0..column_width {
                let value = read_u8(reader)?;

                column.push(value);
            }

            data.push(Some(column));
        }

        Ok(Row { null_values, data })
    }

    fn read_bitfield(
        mut reader: &mut impl Read,
        column_widths: &Vec<u32>,
    ) -> Result<Vec<bool>, Box<dyn Error>> {
        let mut null_values: Vec<bool> = vec![];

        let bitfield_length =
            (column_widths.len() / 8) + if column_widths.len() % 8 == 0 { 0 } else { 1 };
        let bitfield = read_variable(&mut reader, bitfield_length as usize)?;

        for byte in bitfield.iter() {
            for i in (0..8).rev() {
                null_values.push(byte & (1 << i) != 0);
            }
        }

        Ok(null_values)
    }

    pub fn generate_output(
        &self,
        types: &ColumnTypes,
        tz_offset: i8,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let mut record: Vec<String> = vec![];

        for (index, column) in self.data.iter().enumerate() {
            let column_conversion = &types.column_conversions[index];

            let output =
                types.column_types[index].format_value(column, tz_offset, column_conversion);

            record.push(output);
        }

        Ok(record)
    }

    pub fn generate_json_output(
        &self,
        types: &ColumnTypes,
        tz_offset: i8,
    ) -> Result<String, Box<dyn Error>> {
        let mut record = HashMap::new();

        for (index, column) in self.data.iter().enumerate() {
            let column_conversion = &types.column_conversions[index];

            let mapped_value = if column.is_some() {
                match types.column_types[index] {
                    ColumnType::Integer => {
                        let num = convert_to_int(column.as_ref());
                        Value::Number(Number::from(num))
                    }
                    ColumnType::Numeric => {
                        let value = types.column_types[index].format_value(
                            column,
                            tz_offset,
                            column_conversion,
                        );
                        let num = value.parse::<i64>().unwrap();
                        Value::Number(Number::from(num))
                    }
                    ColumnType::Float => {
                        let num = convert_to_float(column.as_ref());
                        Value::Number(Number::from_f64(num).unwrap())
                    }
                    ColumnType::Char
                    | ColumnType::Varchar
                    | ColumnType::Date
                    | ColumnType::Timestamp
                    | ColumnType::TimestampTz
                    | ColumnType::Time
                    | ColumnType::TimeTz
                    | ColumnType::Varbinary
                    | ColumnType::Binary
                    | ColumnType::Interval
                    | ColumnType::UUID => Value::String(types.column_types[index].format_value(
                        column,
                        tz_offset,
                        column_conversion,
                    )),
                    ColumnType::Boolean => Value::Bool(
                        types.column_types[index].format_value(
                            column,
                            tz_offset,
                            column_conversion,
                        ) == "true",
                    ),
                }
            } else {
                Value::Null
            };

            record.insert(&types.column_names[index], mapped_value);
        }

        let str_record = serde_json::to_string(&record).unwrap();

        Ok(str_record)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::BufReader;

    use crate::vertica_native_file::VerticaNativeFile;

    #[test]
    fn test_read_from_good_file() {
        let mut file = BufReader::new(File::open("data/all-types.bin").unwrap());

        let file = VerticaNativeFile::from_reader(&mut file).unwrap();

        for row in file {
            assert_eq!(14, row.data.len());
        }
    }

    #[test]
    fn test_read_from_good_file_with_nulls() {
        let mut file = BufReader::new(File::open("data/all-types-with-nulls.bin").unwrap());

        let file = VerticaNativeFile::from_reader(&mut file).unwrap();

        for row in file {
            assert_eq!(14, row.data.len());
        }
    }
}
