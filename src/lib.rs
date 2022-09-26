use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{stdout, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::str::FromStr;

use flate2::write::GzEncoder;
use flate2::Compression;

use column_types::ColumnTypes;
use vertica_native_file::VerticaNativeFile;

mod column_conversion;
mod column_definitions;
mod column_type;
mod column_types;
mod file_signature;
mod vertica_native_file;

/// Read a variable number of bytes from the stream, and return it as a `Vec<u8>`
///
/// * `reader` - something implementing `Read` to read from
/// * `length` - the number of bytes to read
fn read_variable(reader: &mut impl Read, length: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut vec = vec![0u8; length];
    reader.read_exact(vec.as_mut_slice())?;

    Ok(vec)
}

/// Read 4 bytes from the stream, and convert it to a u32
///
/// * `reader` - something implementing `Read` to read from
fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes: [u8; 4] = [0; 4];

    reader.read_exact(&mut bytes)?;

    Ok(u32::from_le_bytes(bytes))
}

/// Read 2 bytes from the stream, and convert it to a u16
///
/// * `reader` - something implementing `Read` to read from
fn read_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut bytes: [u8; 2] = [0; 2];

    reader.read_exact(&mut bytes)?;

    Ok(u16::from_le_bytes(bytes))
}

/// Read 1 bytes from the stream, and return it
///
/// * `reader` - something implementing `Read` to read from
fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut bytes: [u8; 1] = [0; 1];

    reader.read_exact(&mut bytes)?;

    Ok(u8::from_le_bytes(bytes))
}

/// The start of the actual file processing.
///
/// * `input` - the name of the Vertica native file to read from
/// * `output` - the name of the file to write, or `None` for `stdout`
/// * `types` - the name of the type specification file
/// * `tz_offset` - the number of hours to offset a time, or `None` for 0
/// * `quote` - what to use instead of `"` for quoted strings
/// * `delimiter` - what to use instead of `,` for CSV files
/// * `no_header` - whether to include a header for CSV files
/// * `is_json` - write a JSON file instead of CSV
/// * `is_gzip` - gzip the output
/// * `is_json_lines` - write a [JSON-lines](https://jsonlines.org) file instead of CSV or regular JSON
/// * `limit` - stop after `limit` rows
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
    is_json_lines: bool,
    limit: usize,
) -> Result<(), String> {
    if !Path::new(input.as_str()).exists() {
        return Err(format!("input file {} does not exist", input));
    }

    let mut input_file = match File::open(&input) {
        Ok(file) => BufReader::new(file),
        Err(e) => return Err(e.to_string()),
    };

    let types_reader = BufReader::new(File::open(types).unwrap());

    // Read in the column type specification from the file. If this load fails, we abort,
    // because we can't proceed without this information.
    let types = match ColumnTypes::from_reader(types_reader) {
        Ok(types) => types,
        Err(e) => {
            return Err(format!("parsing column types: {}", e));
        }
    };

    // If no offset was passed in, we'll use 0.
    let tz_offset = match tz_offset {
        None => 0i8,
        Some(s) => i8::from_str(&s).unwrap_or(0i8),
    };

    // This line takes the input file, parses the headers, and gets ready to start retrieving
    // rows.
    let native_file = match VerticaNativeFile::from_reader(&mut input_file) {
        Ok(i) => i,
        Err(e) => return Err(e.to_string()),
    };

    // If no output file is specified, we will use `stdout`. In both cases, if the user
    // passed in `-g`, we will gzip the output. If the user specified the same file name
    // for input and output files, we abort.
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

    return if is_json || is_json_lines {
        process_json_file(
            native_file,
            &mut base_writer,
            types,
            tz_offset,
            is_json_lines,
            limit,
        )
    } else {
        process_csv_file(
            native_file,
            base_writer,
            types,
            tz_offset,
            quote,
            delimiter,
            no_header,
            limit,
        )
    };
}

/// Read all the rows of the Vertica native binary file, and write them out
/// in CSV format.
///
/// * `native_file` - the Vertica native binary file
/// * `writer` - the output; either a file, or `stdout`
/// * `types` - the struct containing the column type info
/// * `tz_offset` - number of hours to offset times
/// * `quote` - what to use instead of `"` for quoted strings
/// * `delimeter` - what to use instead of `,`
/// * `no_header` - don't include the header row
/// * `limit` - stop after `limit` rows
fn process_csv_file(
    native_file: VerticaNativeFile,
    writer: BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    tz_offset: i8,
    quote: u8,
    delimiter: u8,
    no_header: bool,
    limit: usize,
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

    // Loop over every row in the Vertica file, writing out a CSV row for each one.
    for (i, row) in native_file.enumerate() {
        // Stop after `limit` rows
        if i >= limit {
            break;
        }

        match row.generate_csv_output(&types, tz_offset) {
            Ok(record) => match &csv_writer.write_record(&record[..]) {
                Ok(_) => {}
                Err(e) => eprintln!("error: {}", e),
            },
            Err(e) => eprintln!("error: {}", e),
        }
    }

    Ok(())
}

/// Read all the rows of the Vertica native binary file, and write them out
/// in JSON (or JSON-lines) format.
///
/// * `native_file` - the Vertica native binary file
/// * `writer` - the output; either a file, or `stdout`
/// * `types` - the struct containing the column type info
/// * `tz_offset` - number of hours to offset times
/// * `is_json_lines` - use [JSON-lines](https://jsonlines.org) format, instead of regular JSON
/// * `limit` - stop after `limit` rows
fn process_json_file(
    native_file: VerticaNativeFile,
    writer: &mut BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    tz_offset: i8,
    is_json_lines: bool,
    limit: usize,
) -> Result<(), String> {
    // Unlike CSV files, which can be written without a header row containing column names, JSON
    // files require them.
    if !types.has_names() {
        return Err("JSON files require column names in types file".to_string());
    }

    // If the output is not a JSON-lines file, we will create a top-level array,
    // and include each row inside that, separated by a comma.
    if !is_json_lines {
        write_json_row(writer, "[".as_bytes());
    }

    for (i, row) in native_file.enumerate() {
        // Stop after `limit` rows
        if i >= limit {
            break;
        }

        // If the output is not a JSON-lines file, we print a comma before every record, after
        // the first.
        if i > 0 && !is_json_lines {
            write_json_row(writer, ",".as_bytes());
        }

        match row.generate_json_output(&types, tz_offset) {
            Ok(record) => write_json_row(writer, record.as_bytes()),
            Err(e) => eprintln!("error: {}", e),
        }

        // If the output is a JSON-lines file, we need to append a newline after each object.
        if is_json_lines {
            write_json_row(writer, "\n".as_bytes());
        }
    }

    // If the output is not a JSON-lines file, we need to close the array at the end.
    if !is_json_lines {
        write_json_row(writer, "]\n".as_bytes());
    }

    return Ok(());
}

/// Convenience function to DRY-ly write a byte-array to the JSON file
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
    use std::io::{BufRead, BufReader};

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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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
                false,
                usize::MAX,
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

    #[test]
    fn test_json_lines_file() {
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
                true,
                usize::MAX,
            );

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents["The_Date"].as_str().unwrap(), "1999-01-08");
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }

    #[test]
    fn test_csv_file_row_limit() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types-ten-rows.bin");
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
                false,
                5_usize,
            );

            assert!(result.is_ok());

            let f = File::open(&output_file_name).unwrap();

            let mut csv_file = csv::ReaderBuilder::new().has_headers(true).from_reader(f);

            let records: Vec<StringRecord> = csv_file.records().map(|r| r.unwrap()).collect();

            assert!(csv_file.has_headers());
            assert_eq!(records.len(), 5_usize);

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
    fn test_json_lines_with_row_limit() {
        let output_file_name = format!(
            "{}/{}.json",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types-ten-rows.bin");
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
                true,
                5_usize,
            );

            assert!(result.is_ok());
            let f = BufReader::new(File::open(&output_file_name).unwrap());

            let contents: Vec<serde_json::Value> = f.lines().map(|row|  {
                serde_json::from_str(row.unwrap().as_str()).unwrap()
            }).collect();

            assert_eq!(contents.len(), 5_usize);
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
    fn test_json_file_with_row_limit() {
        let output_file_name = format!(
            "{}/{}.json",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let input_file_name = String::from("data/all-types-ten-rows.bin");
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
                false,
                5_usize,
            );

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: serde_json::Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents.as_array().unwrap().len(), 5_usize);
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
