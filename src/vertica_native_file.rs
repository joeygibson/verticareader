use std::collections::HashMap;
use std::io::Read;

use serde_json::{Number, Value};

use crate::column_definitions::ColumnDefinitions;
use crate::column_type::ColumnType;
use crate::column_types::ColumnTypes;
use crate::file_signature::FileSignature;
use crate::{read_u32, read_variable, Args};

/// The [Vertica native binary](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm)
/// is a compact, structured, binary file format for copy large amounts of data into the Vertica
/// database. The file structure can be found at the above link. This struct contains all the
/// metadata read from the header, and provides an iterator to walk through the individual rows
/// of data.
///
/// A diagram of the header layout can be found [here](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/FileSignature.htm).
///
pub struct VerticaNativeFile<'a> {
    /// The stock file signature. It's not used, but we still needed to read it.
    _signature: FileSignature,
    /// The definitions for all the columns
    pub definitions: ColumnDefinitions,
    /// The input source of the file
    file: &'a mut dyn Read,
}

impl<'a> VerticaNativeFile<'a> {
    /// Create the struct from the `reader`
    pub fn from_reader(reader: &'a mut impl Read) -> anyhow::Result<Self> {
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

    /// Iterate through all the rows of the native file, returning them for further processing.
    fn next(&mut self) -> Option<Self::Item> {
        // First, read a `u32` which gives the length of the row, not including the length,
        // or the bitfield indicating null values.
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
/// A struct containing a single row of data from the native file.
///
/// The layout of the the bytes for each row can be found [here](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/RowData.htm).
///
pub struct Row {
    pub null_values: Vec<bool>,
    pub data: Vec<Option<Vec<u8>>>,
}

impl Row {
    /// Create a `Row` from the binary file.
    fn from_reader(reader: &mut impl Read, column_widths: &Vec<u32>) -> anyhow::Result<Self> {
        let mut data: Vec<Option<Vec<u8>>> = vec![];

        // After the length field, is one or more bytes that represent a bit field,
        // which indicates which, if any, of the columns are actually null, and therefore,
        // not present.
        let null_values = Row::read_bitfield(reader, &column_widths)?;

        // Loop over each column definition from the header, and attempt to read that column
        // for the specific row
        for (index, width) in column_widths.iter().enumerate() {
            // If the bitmap indicates that for this row, this column is null, we just
            // push a `None` into the vector, since we need _something_ there, even for
            // nothing.
            if null_values[index] {
                data.push(None);
                continue;
            }

            // let mut column: Vec<u8> = vec![];

            // If the width for this column is `u32::MAX`, that means it's a variable-width field.
            // In this case, we need to read a single `u32`, which then tells us how many bytes
            // to read for the column.
            let column_width = if *width == u32::MAX {
                read_u32(reader)?
            } else {
                *width
            };

            // Now that we know how many bytes to read for this column, let's read them in.
            let column = read_variable(reader, column_width as usize)?;

            // And finally wrap the vector in a `Some`, and push it into the row vector.
            data.push(Some(column));
        }

        Ok(Row { null_values, data })
    }

    /// After the length value at the beginning of a row is one or more bytes that represent
    /// a bitfield. This bitfield is used to show which columns are null for this row. A 1
    /// means a column is `null` in this row.
    fn read_bitfield(
        mut reader: &mut impl Read,
        column_widths: &Vec<u32>,
    ) -> anyhow::Result<Vec<bool>> {
        let mut null_values: Vec<bool> = vec![];

        // The number of bytes in the bitfield is based on the number of columns, so we have
        // to compute it.
        let bitfield_length =
            (column_widths.len() / 8) + if column_widths.len() % 8 == 0 { 0 } else { 1 };
        let bitfield = read_variable(&mut reader, bitfield_length as usize)?;

        // Now, loop over each bit in the bitfield, pushing a `true` for `null`s, and a `false`
        // for present values.
        for byte in bitfield.iter() {
            for i in (0..8).rev() {
                null_values.push(byte & (1 << i) != 0);
            }
        }

        Ok(null_values)
    }

    /// Take a row of data and generate a CSV representation of it.
    ///
    /// * `types` - the ColumnTypes struct with conversion info
    /// * `tz_offset` - number of hours to offset times
    pub fn generate_csv_output(
        &self,
        types: &ColumnTypes,
        tz_offset: i8,
        args: &Args,
    ) -> anyhow::Result<Vec<String>> {
        let mut record: Vec<String> = vec![];

        // Loop over each column, format it, and push it into the vector.
        for (index, column) in self.data.iter().enumerate() {
            let column_conversion = &types.column_conversions[index];

            let output =
                types.column_types[index].format_value(column, tz_offset, column_conversion, &args);

            record.push(output);
        }

        Ok(record)
    }

    /// Take a row of data and generate a JSON representation of it.
    ///
    /// * `types` - the ColumnTypes struct with conversion info
    /// * `tz_offset` - number of hours to offset times
    pub fn generate_json_output(
        &self,
        types: &ColumnTypes,
        tz_offset: i8,
        args: &Args,
    ) -> anyhow::Result<String> {
        let mut record = HashMap::new();

        for (index, column) in self.data.iter().enumerate() {
            let column_conversion = &types.column_conversions[index];

            let name = types.column_names[index].clone();
            let value =
                types.column_types[index].format_value(column, tz_offset, column_conversion, &args);

            // Generating JSON is more involved than CSV, and the `serde_json` crate requires
            // wrapping values in a struct that indicates its actual type. So we need to map
            // Vertica types into `serde_json` types.
            let mapped_value = match types.column_types[index] {
                ColumnType::Integer | ColumnType::Numeric => {
                    if value.is_empty() {
                        Value::Null
                    } else {
                        let num = value.parse::<i64>().unwrap();
                        Value::Number(Number::from(num))
                    }
                }
                ColumnType::Float => {
                    let num = value.parse::<f64>().unwrap();
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
                | ColumnType::UUID => Value::String(value),
                ColumnType::Boolean => Value::Bool(value == "1"),
            };

            record.insert(name, mapped_value);
        }

        // Use the `serde_json` crate to convert the `HashMap` into a JSON string
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
