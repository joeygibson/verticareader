use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{stdout, BufWriter, Read, Write};
use std::path::Path;
use std::str::FromStr;

use column_types::ColumnTypes;
use vertica_native_file::VerticaNativeFile;

mod column_conversion;
mod column_definitions;
mod column_type;
mod column_types;
mod file_signature;
mod vertica_native_file;

fn read_variable(reader: &mut impl Read, length: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut vec = vec![0u8; length];
    reader.read(vec.as_mut_slice())?;

    Ok(vec)
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes: [u8; 4] = [0; 4];

    let res = reader.read(&mut bytes)?;

    if res != 4 {
        Ok(0)
    } else {
        Ok(u32::from_le_bytes(bytes))
    }
}

fn read_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut bytes: [u8; 2] = [0; 2];

    reader.read_exact(&mut bytes)?;

    Ok(u16::from_le_bytes(bytes))
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut bytes: [u8; 1] = [0; 1];

    reader.read_exact(&mut bytes)?;

    Ok(u8::from_le_bytes(bytes))
}

pub fn process_file(
    input: String,
    output: Option<&str>,
    types: String,
    tz_offset: Option<&str>,
    quote: u8,
    delimiter: u8,
    no_header: bool,
) -> Result<(), String> {
    if !Path::new(input.as_str()).exists() {
        return Err(format!("input file {} does not exist", input));
    }

    let mut input_file = match File::open(&input) {
        Ok(i) => i,
        Err(e) => return Err(e.to_string()),
    };

    let types_reader = File::open(types).unwrap();

    let types = match ColumnTypes::from_reader(&types_reader) {
        Ok(types) => types,
        Err(e) => {
            return Err(format!("parsing column types: {}", e));
        }
    };

    let native_file = match VerticaNativeFile::from_reader(&mut input_file) {
        Ok(i) => i,
        Err(e) => return Err(e.to_string()),
    };

    let writer: BufWriter<Box<dyn Write>> = BufWriter::new(if let Some(filename) = output {
        if filename == input {
            return Err("can't overwrite input file".to_string());
        }

        Box::new(File::create(filename).unwrap())
    } else {
        Box::new(stdout())
    });

    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .from_writer(writer);

    let tz_offset = match tz_offset {
        None => 0i8,
        Some(s) => i8::from_str(&s).unwrap_or(0i8),
    };

    if !no_header {
        if !&types.column_names.iter().all(|n| n == "") {
            &csv_writer.write_record(&types.column_names[..]);
        }
    }

    for row in native_file {
        match row.generate_output(&types, tz_offset) {
            Ok(record) => match &csv_writer.write_record(&record[..]) {
                Ok(_) => {}
                Err(e) => eprintln!("error: {}", e),
            },
            Err(e) => eprintln!("error: {}", e),
        }
    }

    Ok(())
}
