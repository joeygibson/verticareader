use std::io::{Read, Result};

use crate::{read_u16, read_u32, read_u8};

#[derive(Debug)]
#[allow(unused)]
pub struct ColumnDefinitions {
    header_length: u32,
    version: u16,
    // filler
    number_of_columns: u16,
    pub column_widths: Vec<u32>,
}

impl ColumnDefinitions {
    pub fn from_reader(reader: &mut impl Read) -> Result<Self> {
        let header_length: u32 = read_u32(reader)?;
        let version = read_u16(reader)?;

        // drop the filler
        read_u8(reader)?;

        let number_of_columns = read_u16(reader)?;

        let mut column_widths: Vec<u32> = vec![];

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
