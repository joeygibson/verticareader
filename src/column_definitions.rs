use std::io::Read;

use crate::{read_u16, read_u32, read_u8};

#[derive(Debug)]
#[allow(unused)]
/// A struct that contains the definitions of all the columns in the file.
/// The byte layout of this section is described [here](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/ColumnDefinitions.htm).
///
pub struct ColumnDefinitions {
    header_length: u32,
    version: u16,
    // filler
    number_of_columns: u16,
    pub column_widths: Vec<u32>,
}

impl ColumnDefinitions {
    pub fn from_reader(reader: &mut impl Read) -> anyhow::Result<Self> {
        let header_length: u32 = read_u32(reader)?;
        let version = read_u16(reader)?;

        // drop the filler
        read_u8(reader)?;

        // Read the number of columns that make up each row (even if a given row doens't have
        // a value for a particular column.
        let number_of_columns = read_u16(reader)?;

        let mut column_widths: Vec<u32> = vec![];

        // Each column's width is given as a `u32`. For variable-width fields (varchar, etc.), the
        // value here is `u32::MAX`. This indicates that as a row is being processed, when the
        // parser gets to this column, it needs to read a `u32` at that point, to get the actual
        // column size.
        for _ in 0..number_of_columns {
            let value = read_u32(reader)?;
            column_widths.push(value);
        }

        Ok(ColumnDefinitions {
            header_length,
            version,
            number_of_columns,
            column_widths,
        })
    }
}

#[cfg(test)]
mod tests {
    // use std::io::{Seek, SeekFrom};

    use std::io::{BufReader, Seek, SeekFrom};

    use crate::column_definitions::ColumnDefinitions;

    #[test]
    fn test_read_from_good_file() {
        use std::fs::File;

        let mut file = BufReader::new(File::open("data/all-types.bin").unwrap());

        file.seek(SeekFrom::Start(11)).unwrap();

        let expected_column_widths: [u32; 14] =
            [8, 8, 10, 4294967295, 1, 8, 8, 8, 8, 8, 4294967295, 3, 24, 8];

        let column_definitions = ColumnDefinitions::from_reader(&mut file).unwrap();

        for (index, expected_value) in expected_column_widths.iter().enumerate() {
            let value = column_definitions.column_widths[index];

            assert_eq!(*expected_value, value);
        }
    }
}
