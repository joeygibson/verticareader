use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{stdout, BufReader, BufWriter, Read, Write};
use std::path::Path;

use flate2::write::GzEncoder;
use flate2::Compression;

use column_types::ColumnTypes;
use vertica_native_file::VerticaNativeFile;

use crate::args::Args;

pub mod args;
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
/// * `args` - all the command line arguments
pub fn process_file(args: Args) -> Result<(), String> {
    if !Path::new(&args.input.as_str()).exists() {
        return Err(format!("input file {} does not exist", args.input));
    }

    let mut input_file = match File::open(&args.input) {
        Ok(file) => BufReader::new(file),
        Err(e) => return Err(e.to_string()),
    };

    let types_reader = BufReader::new(File::open(&args.types).unwrap());

    // Read in the column type specification from the file. If this load fails, we abort,
    // because we can't proceed without this information.
    let types = match ColumnTypes::from_reader(types_reader) {
        Ok(types) => types,
        Err(e) => {
            return Err(format!("parsing column types: {}", e));
        }
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
        BufWriter::new(if let Some(filename) = &args.output {
            if filename == &args.input {
                return Err("can't overwrite input file".to_string());
            }

            let tmp_writer = File::create(filename).unwrap();

            if args.is_gzip {
                Box::new(GzEncoder::new(tmp_writer, Compression::default()))
            } else {
                Box::new(tmp_writer)
            }
        } else {
            Box::new(stdout())
        });

    return if args.is_json || args.is_json_lines {
        process_json_file(native_file, &mut base_writer, types, &args)
    } else {
        process_csv_file(native_file, base_writer, types, args)
    };
}

/// Read all the rows of the Vertica native binary file, and write them out
/// in CSV format.
///
/// * `native_file` - the Vertica native binary file
/// * `writer` - the output; either a file, or `stdout`
/// * `types` - the struct containing the column type info
/// * `args` - all the other command line arguments
fn process_csv_file(
    native_file: VerticaNativeFile,
    writer: BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    args: Args,
) -> Result<(), String> {
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(args.delimiter)
        .quote(if args.single_quotes { b'\'' } else { b'\"' })
        .from_writer(writer);

    if !args.no_header {
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
        if i >= args.limit {
            break;
        }

        match row.generate_csv_output(&types, args.tz_offset, &args) {
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
/// * `args` - all the other command line arguments
fn process_json_file(
    native_file: VerticaNativeFile,
    writer: &mut BufWriter<Box<dyn Write>>,
    types: ColumnTypes,
    args: &Args,
) -> Result<(), String> {
    // Unlike CSV files, which can be written without a header row containing column names, JSON
    // files require them.
    if !types.has_names() {
        return Err("JSON files require column names in types file".to_string());
    }

    // If the output is not a JSON-lines file, we will create a top-level array,
    // and include each row inside that, separated by a comma.
    if !args.is_json_lines {
        write_json_row(writer, "[".as_bytes());
    }

    for (i, row) in native_file.enumerate() {
        // Stop after `limit` rows
        if i >= args.limit {
            break;
        }

        // If the output is not a JSON-lines file, we print a comma before every record, after
        // the first.
        if i > 0 && !args.is_json_lines {
            write_json_row(writer, ",".as_bytes());
        }

        match row.generate_json_output(&types, args.tz_offset, args) {
            Ok(record) => write_json_row(writer, record.as_bytes()),
            Err(e) => {
                eprintln!("error: {}", e);
                continue;
            },
        }

        // If the output is a JSON-lines file, we need to append a newline after each object.
        if args.is_json_lines {
            write_json_row(writer, "\n".as_bytes());
        }
    }

    // If the output is not a JSON-lines file, we need to close the array at the end.
    if !args.is_json_lines {
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
    use std::io::{BufRead, BufReader};
    use std::path::Path;
    use std::{fs, panic};

    use csv::StringRecord;
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::{process_file, Args};

    #[test]
    fn test_csv_file_with_no_headers() {
        let output_file_name = format!(
            "{}/{}.csv",
            temp_dir().to_str().unwrap(),
            Uuid::new_v4().to_string()
        );

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types.txt"),
        );

        args.no_header = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.no_header = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types.txt"),
        );
        args.is_json = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );
        args.is_json = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents[0]["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents[0]["The_Date"].as_str().unwrap(), "1999-01-08");
            assert_eq!(contents[0]["Bools"].as_bool().unwrap(), true);
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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.is_gzip = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types-ten-rows.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.is_json = true;
        args.is_gzip = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );
        args.is_json_lines = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents["The_Date"].as_str().unwrap(), "1999-01-08");
            assert_eq!(contents["Bools"].as_bool().unwrap(), true);
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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types-ten-rows.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.limit = 5_usize;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types-ten-rows.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );
        args.is_json_lines = true;
        args.limit = 5_usize;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

            assert!(result.is_ok());
            let f = BufReader::new(File::open(&output_file_name).unwrap());

            let contents: Vec<serde_json::Value> = f
                .lines()
                .map(|row| serde_json::from_str(row.unwrap().as_str()).unwrap())
                .collect();

            assert_eq!(contents.len(), 5_usize);
            assert_eq!(contents[0]["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents[0]["The_Date"].as_str().unwrap(), "1999-01-08");
            assert_eq!(contents[0]["Bools"].as_bool().unwrap(), true);
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

        let mut args = Args::with_most_defaults(
            String::from("data/all-types-ten-rows.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.is_json = true;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

            assert!(result.is_ok());
            let f = File::open(&output_file_name).unwrap();

            let contents: serde_json::Value = serde_json::from_reader(f).unwrap();

            assert_eq!(contents.as_array().unwrap().len(), 5_usize);
            assert_eq!(contents[0]["IntCol"].as_i64().unwrap(), 1);
            assert_eq!(contents[0]["The_Date"].as_str().unwrap(), "1999-01-08");
            assert_eq!(contents[0]["Bools"].as_bool().unwrap(), true);
        });

        match fs::remove_file(Path::new(&output_file_name)) {
            Ok(_) => {}
            Err(e) => eprintln!("error removing {}, {}", &output_file_name, e),
        }

        assert!(rc.is_ok());
    }
}
