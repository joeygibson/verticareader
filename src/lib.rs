use flate2::write::GzEncoder;
use flate2::Compression;
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{stdout, BufReader, BufWriter, Read, Write};
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
    reader.read_exact(vec.as_mut_slice())?;

    Ok(vec)
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes: [u8; 4] = [0; 4];

    reader.read_exact(&mut bytes)?;

    Ok(u32::from_le_bytes(bytes))
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
    is_json: bool,
    is_gzip: bool,
) -> Result<(), String> {
    if !Path::new(input.as_str()).exists() {
        return Err(format!("input file {} does not exist", input));
    }

    let mut input_file = match File::open(&input) {
        Ok(file) => BufReader::new(file),
        Err(e) => return Err(e.to_string()),
    };

    let types_reader = BufReader::new(File::open(types).unwrap());

    let types = match ColumnTypes::from_reader(types_reader) {
        Ok(types) => types,
        Err(e) => {
            return Err(format!("parsing column types: {}", e));
        }
    };

    let tz_offset = match tz_offset {
        None => 0i8,
        Some(s) => i8::from_str(&s).unwrap_or(0i8),
    };

    let native_file = match VerticaNativeFile::from_reader(&mut input_file) {
        Ok(i) => i,
        Err(e) => return Err(e.to_string()),
    };

    let mut base_writer: BufWriter<Box<dyn Write>> =
        BufWriter::new(if let Some(filename) = output {
            if filename == input {
                return Err("can't overwrite input file".to_string());
            }

            let tmp_writer = File::create(filename).unwrap();

            if is_gzip {
                Box::new(GzEncoder::new(tmp_writer, Compression::default()))
            } else {
                Box::new(tmp_writer)
            }
        } else {
            Box::new(stdout())
        });

    return if is_json {
        process_json_file(native_file, &mut base_writer, types, tz_offset)
    } else {
        process_csv_file(
            native_file,
            base_writer,
            types,
            tz_offset,
            quote,
            delimiter,
            no_header,
        )
    };
}

fn process_csv_file(
    native_file: VerticaNativeFile,
    writer: BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    tz_offset: i8,
    quote: u8,
    delimiter: u8,
    no_header: bool,
) -> Result<(), String> {
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .from_writer(writer);

    if !no_header {
        if types.has_names() {
            match csv_writer.write_record(&types.column_names[..]) {
                Ok(_) => {}
                Err(e) => eprintln!("error writing CSV header: {}", e),
            }
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

fn process_json_file(
    native_file: VerticaNativeFile,
    writer: &mut BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    tz_offset: i8,
) -> Result<(), String> {
    if !types.has_names() {
        return Err("JSON files require column names in types file".to_string());
    }

    write_json_row(writer, "[".as_bytes());

    for (i, row) in native_file.enumerate() {
        if i > 0 {
            write_json_row(writer, ",".as_bytes());
        }

        match row.generate_json_output(&types, tz_offset) {
            Ok(record) => write_json_row(writer, record.as_bytes()),
            Err(e) => eprintln!("error: {}", e),
        }
    }

    write_json_row(writer, "]\n".as_bytes());

    return Ok(());
}

fn write_json_row(writer: &mut BufWriter<Box<dyn Write>>, buf: &[u8]) {
    match writer.write_all(buf) {
        Ok(_) => {}
        Err(e) => eprintln!("error: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::File;
    use std::path::Path;
    use std::{fs, panic};

    use csv::StringRecord;
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::process_file;

    #[test]
    fn test_csv_file_with_no_headers() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                false,
                false,
            );

            assert!(result.is_ok());

            let f = File::open(&output_file_name).unwrap();

            let mut csv_file = csv::ReaderBuilder::new().has_headers(false).from_reader(f);

            let records: Vec<StringRecord> = csv_file.records().map(|r| r.unwrap()).collect();

            assert!(!csv_file.has_headers());
            assert_eq!(records.len(), 1_usize);

            assert_eq!(records[0].len(), 14_usize);
            assert_eq!(records[0][0].to_string(), "1");
            assert_eq!(records[0][5].to_string(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_csv_file_with_headers() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types-with-names.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                false,
                false,
            );

            assert!(result.is_ok());

            let f = File::open(&output_file_name).unwrap();

            let mut csv_file = csv::ReaderBuilder::new().has_headers(true).from_reader(f);

            let records: Vec<StringRecord> = csv_file.records().map(|r| r.unwrap()).collect();

            assert!(csv_file.has_headers());
            assert_eq!(records.len(), 1_usize);

            assert_eq!(records[0].len(), 14_usize);
            assert_eq!(records[0][0].to_string(), "1");
            assert_eq!(records[0][5].to_string(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_csv_file_with_headers_but_turned_off() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types-with-names.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                true,
                false,
                false,
            );

            assert!(result.is_ok());

            let f = File::open(&output_file_name).unwrap();

            let mut csv_file = csv::ReaderBuilder::new().has_headers(false).from_reader(f);

            let records: Vec<StringRecord> = csv_file.records().map(|r| r.unwrap()).collect();

            assert!(!csv_file.has_headers());
            assert_eq!(records.len(), 1_usize);

            assert_eq!(records[0].len(), 14_usize);
            assert_eq!(records[0][0].to_string(), "1");
            assert_eq!(records[0][5].to_string(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_json_file_with_missing_column_names() {
        let output_file_name = format!(
            "{}/{}.json",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                true,
                false,
            );

            assert!(result.is_err());
            assert_eq!(
                result.err().unwrap(),
                "JSON files require column names in types file".to_string()
            );
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_json_file() {
        let output_file_name = format!(
            "{}/{}.json",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types-with-names.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                true,
                false,
            );

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents[0]["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents[0]["The_Date"].as_str().unwrap(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_gzipped_csv_file_with_headers() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types-with-names.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                false,
                true,
            );

            assert!(result.is_ok());

            let f = GzDecoder::new(File::open(&output_file_name).unwrap());

            let mut csv_file = csv::ReaderBuilder::new().has_headers(true).from_reader(f);

            let records: Vec<StringRecord> = csv_file.records().map(|r| r.unwrap()).collect();

            assert!(csv_file.has_headers());
            assert_eq!(records.len(), 1_usize);

            assert_eq!(records[0].len(), 14_usize);
            assert_eq!(records[0][0].to_string(), "1");
            assert_eq!(records[0][5].to_string(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_gzipped_json_file() {
        let output_file_name = format!(
            "{}/{}.json",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types.bin");
        let types_file_name = String::from("data/all-valid-types-with-names.txt");

        let rc = panic::catch_unwind(|| {
            let result = process_file(
                input_file_name,
                Some(output_file_name.as_str()),
                types_file_name,
                None,
                b'"',
                b',',
                false,
                true,
                true,
            );

            assert!(result.is_ok());
            let f = GzDecoder::new(File::open(&output_file_name).unwrap());

            let contents: Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents[0]["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents[0]["The_Date"].as_str().unwrap(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }
}
