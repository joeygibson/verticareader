use std::fs::File;
use std::io::{stdout, BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{bail, Context};
use csv::Writer;
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
fn read_variable(reader: &mut impl Read, length: usize) -> anyhow::Result<Vec<u8>> {
    let mut vec = vec![0u8; length];
    reader.read_exact(vec.as_mut_slice())?;

    Ok(vec)
}

/// Read 4 bytes from the stream, and convert it to a u32
///
/// * `reader` - something implementing `Read` to read from
fn read_u32(reader: &mut impl Read) -> anyhow::Result<u32> {
    let mut bytes: [u8; 4] = [0; 4];

    reader.read_exact(&mut bytes)?;

    Ok(u32::from_le_bytes(bytes))
}

/// Read 2 bytes from the stream, and convert it to a u16
///
/// * `reader` - something implementing `Read` to read from
fn read_u16(reader: &mut impl Read) -> anyhow::Result<u16> {
    let mut bytes: [u8; 2] = [0; 2];

    reader.read_exact(&mut bytes)?;

    Ok(u16::from_le_bytes(bytes))
}

/// Read 1 bytes from the stream, and return it
///
/// * `reader` - something implementing `Read` to read from
fn read_u8(reader: &mut impl Read) -> anyhow::Result<u8> {
    let mut bytes: [u8; 1] = [0; 1];

    reader.read_exact(&mut bytes)?;

    Ok(u8::from_le_bytes(bytes))
}

/// The start of the actual file processing.
///
/// * `args` - all the command line arguments
pub fn process_file(args: Args) -> anyhow::Result<()> {
    let mut input_file = match File::open(&args.input) {
        Ok(file) => BufReader::new(file),
        Err(e) => bail!("opening input file [{}]: {}", args.input, e),
    };

    let types_reader = match File::open(&args.types) {
        Ok(file) => BufReader::new(file),
        Err(e) => bail!("opening types file [{}]: {}", args.types, e),
    };

    // Read in the column type specification from the file. If this load fails, we abort,
    // because we can't proceed without this information.
    let types = match ColumnTypes::from_reader(types_reader) {
        Ok(types) => types,
        Err(e) => {
            bail!("parsing column types: {}", e);
        }
    };

    // This line takes the input file, parses the headers, and gets ready to start retrieving
    // rows.
    let native_file = VerticaNativeFile::from_reader(&mut input_file).context("creating file")?;

    return if args.is_json || args.is_json_lines {
        process_json_file(native_file, types, &args)
    } else {
        process_csv_file(native_file, types, args)
    };
}

/// Verify that the proposed output file isn't the same as either
/// the input file or the types file
///
/// * `args` - the CLI arguments
/// * `file_name` - the proposed output file name
fn validate_output_file_name_ok(args: &Args, file_name: &String) -> anyhow::Result<()> {
    if file_name == &args.input {
        bail!("can't overwrite input file");
    }

    if file_name == &args.types {
        bail!("can't overwrite types file");
    }

    return Ok(());
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
    types: ColumnTypes,
    args: Args,
) -> anyhow::Result<()> {
    let mut writer = create_csv_file(&args, &types, None)?;

    let mut file_no: usize = 1;
    // Loop over every row in the Vertica file, writing out a CSV row for each one.
    for (i, row) in native_file.enumerate() {
        // Stop after `limit` rows
        if i >= args.limit {
            break;
        }

        if i > 0 && i % args.max_rows == 0 {
            writer = create_csv_file(&args, &types, Some(file_no))?;
            file_no += 1;
        }

        match row.generate_csv_output(&types, args.tz_offset, &args) {
            Ok(record) => match &writer.write_record(&record[..]) {
                Ok(_) => {}
                Err(e) => eprintln!("error: {}", e),
            },
            Err(e) => eprintln!("error: {}", e),
        }
    }

    Ok(())
}

fn create_csv_file(
    args: &Args,
    types: &ColumnTypes,
    iteration: Option<usize>,
) -> anyhow::Result<Writer<BufWriter<Box<dyn Write>>>> {
    let base_writer = create_output_file(&args, iteration)?;
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(args.delimiter)
        .quote(if args.single_quotes { b'\'' } else { b'\"' })
        .from_writer(base_writer);

    if !args.no_header {
        if types.has_names() {
            match csv_writer.write_record(&types.column_names[..]) {
                Ok(_) => {}
                Err(e) => eprintln!("error writing CSV header: {}", e),
            }
        }
    }

    Ok(csv_writer)
}

fn create_output_file(
    args: &Args,
    iteration: Option<usize>,
) -> anyhow::Result<BufWriter<Box<dyn Write>>> {
    let output_file_name = generate_output_file_name(&args, iteration)?;
    validate_output_file_name_ok(&args, &output_file_name)?;
    let writer = open_output_file_name(&args, output_file_name)?;

    Ok(writer)
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
    types: ColumnTypes,
    args: &Args,
) -> anyhow::Result<()> {
    let mut writer = create_output_file(&args, None)?;

    // Unlike CSV files, which can be written without a header row containing column names, JSON
    // files require them.
    if !types.has_names() {
        bail!("JSON files require column names in types file".to_string());
    }

    // If the output is not a JSON-lines file, we will create a top-level array,
    // and include each row inside that, separated by a comma.
    if !args.is_json_lines {
        write_json_row(&mut writer, "[".as_bytes());
    }

    let mut file_no: usize = 1;
    for (i, row) in native_file.enumerate() {
        // Stop after `limit` rows
        if i >= args.limit {
            break;
        }

        if i > 0 && i % args.max_rows == 0 {
            writer = create_output_file(&args, Some(file_no))?;
            file_no += 1;
        }

        // If the output is not a JSON-lines file, we print a comma before every record, after
        // the first.
        if i > 0 && !args.is_json_lines {
            write_json_row(&mut writer, ",".as_bytes());
        }

        match row.generate_json_output(&types, args.tz_offset, args) {
            Ok(record) => write_json_row(&mut writer, record.as_bytes()),
            Err(e) => {
                eprintln!("error: {}", e);
                continue;
            }
        }

        // If the output is a JSON-lines file, we need to append a newline after each object.
        if args.is_json_lines {
            write_json_row(&mut writer, "\n".as_bytes());
        }
    }

    // If the output is not a JSON-lines file, we need to close the array at the end.
    if !args.is_json_lines {
        write_json_row(&mut writer, "]\n".as_bytes());
    }

    return Ok(());
}

/// Generate the output file name, if none given, or return what the user specified.
/// If an `iteration` is given, it will be appended to the end of the file stem, before
/// the extension(s).
///
/// * `args` - the CLI arguments
/// * `iteration` - an `Option<u64>`, that will be appended to the file stem, if present
fn generate_output_file_name(args: &Args, iteration: Option<usize>) -> anyhow::Result<String> {
    // If no output file is specified, we will create a file name based on the input file.
    // If the `iteration` argument is passed, we will append it before the extension(s)
    let file_name = match &args.output {
        None => {
            // User didn't give an output file name, so we will generate it
            let extension = if args.is_json {
                "json"
            } else if args.is_json_lines {
                "jsonl"
            } else {
                "csv"
            };

            let iteration_tag = match iteration {
                None => "".to_string(),
                Some(i) => format!("-{}", i),
            };

            let file_without_directory = Path::new(&args.input)
                .file_name()
                .ok_or("bad output file name")
                .unwrap()
                .to_str()
                .unwrap();
            let base_name = format!("{}{}.{}", file_without_directory, iteration_tag, extension);

            if args.is_gzip {
                format!("{}.gz", base_name)
            } else {
                base_name
            }
        }
        Some(name) => match iteration {
            // User gave an output name, and for the simple case, we just return it. If
            // `iteration` isn't `None`, we need to tear the filename apart, and figure out
            // where to put the iteration.
            None => name.to_string(),
            Some(i) => {
                // Split it on `.`...
                let chunks = name.split('.').collect::<Vec<&str>>();

                // If there are no dots, we slap the iteration on the end of the whole string
                if chunks.len() == 1 {
                    format!("{}-{}", name, i)
                } else {
                    // We need to figure out all the extensions, but don't include any other
                    // chunks separated by `.`.
                    let final_ext = chunks.last().unwrap();
                    let penultimate_ext = if chunks.len() > 2 {
                        match chunks[chunks.len() - 2] {
                            "csv" | "json" | "jsonl" => Some(chunks[chunks.len() - 2]),
                            _ => None,
                        }
                    } else {
                        None
                    };

                    let (base_name, full_extension) = match penultimate_ext {
                        None => {
                            let base_name = chunks[0..chunks.len() - 1].join(".");
                            (base_name.to_string(), final_ext.to_string())
                        }
                        Some(pen_ext) => {
                            let base_name = chunks[0..chunks.len() - 2].join(".");
                            let ext = format!("{}.{}", pen_ext, final_ext);

                            (base_name.to_string(), ext.to_string())
                        }
                    };

                    format!("{}-{}.{}", base_name, i, full_extension)
                }
            }
        },
    };

    Ok(file_name)
}

fn open_output_file_name(
    args: &Args,
    file_name: String,
) -> anyhow::Result<BufWriter<Box<dyn Write>>> {
    // Creates the output file, and return a `BufWriter` on top of it.
    // passed in `-g`, we will gzip the output. If the user specified the same file name
    // for input and output files, we abort.
    let writer = if file_name != "-" {
        let tmp_writer = File::create(file_name)?;

        let base_writer: Box<dyn Write> = if args.is_gzip {
            Box::new(GzEncoder::new(tmp_writer, Compression::default()))
        } else {
            Box::new(tmp_writer)
        };

        base_writer
    } else {
        Box::new(stdout())
    };

    Ok(BufWriter::new(writer))
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
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::{fs, panic};

    use csv::StringRecord;
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::{
        generate_output_file_name, open_output_file_name, process_file,
        validate_output_file_name_ok, Args,
    };

    #[test]
    fn test_output_filename_generation_based_on_input_csv() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.csv")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_json() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json = true;

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.json")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_jsonl() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json_lines = true;

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.jsonl")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_csv_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.csv")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_json_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json = true;

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.json")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_jsonl_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json_lines = true;

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.jsonl")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_csv_gzipped() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.csv.gz")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_json_gzipped() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json = true;
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.json.gz")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_jsonl_gzipped() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json_lines = true;
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.jsonl.gz")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_csv_gzipped_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.csv.gz")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_json_gzipped_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json = true;
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.json.gz")
    }

    #[test]
    fn test_output_filename_generation_based_on_input_jsonl_gzipped_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.is_json_lines = true;
        args.is_gzip = true;

        let file_name = generate_output_file_name(&args, Some(1)).unwrap();
        assert_eq!(file_name, "foo-1.jsonl.gz")
    }

    #[test]
    fn test_output_filename_generation_from_specified() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar.csv".to_string());

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "bar.csv")
    }

    #[test]
    fn test_output_filename_generation_from_specified_with_iteration() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar.csv".to_string());

        let file_name = generate_output_file_name(&args, Some(2)).unwrap();
        assert_eq!(file_name, "bar-2.csv")
    }

    #[test]
    fn test_output_filename_generation_from_specified_with_iteration_and_gzip() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar.csv.gz".to_string());

        let file_name = generate_output_file_name(&args, Some(2)).unwrap();
        assert_eq!(file_name, "bar-2.csv.gz")
    }

    #[test]
    fn test_output_filename_generation_from_specified_with_iteration_and_multiple_dots() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar.foo.quux.csv.gz".to_string());

        let file_name = generate_output_file_name(&args, Some(2)).unwrap();
        assert_eq!(file_name, "bar.foo.quux-2.csv.gz")
    }

    #[test]
    fn test_output_filename_generation_from_specified_with_iteration_and_one_extension() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar.csv".to_string());

        let file_name = generate_output_file_name(&args, Some(2)).unwrap();
        assert_eq!(file_name, "bar-2.csv")
    }

    #[test]
    fn test_output_filename_generation_from_specified_with_iteration_and_no_dots() {
        let mut args = Args::with_defaults();
        args.input = "foo".to_string();
        args.output = Some("bar".to_string());

        let file_name = generate_output_file_name(&args, Some(2)).unwrap();
        assert_eq!(file_name, "bar-2")
    }

    #[test]
    fn test_open_impossible_file() {
        let output_file_name = format!(
            "/lksjdfklsdf/sdfsdf/sdf/sdf/{}.csv",
            Uuid::new_v4().to_string()
        );

        let args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types.txt"),
        );

        let rc = panic::catch_unwind(|| {
            let mut file = open_output_file_name(&args, output_file_name.clone()).unwrap();

            file.write("testing".as_bytes()).unwrap();
            file.flush().unwrap();

            let data = fs::read_to_string(output_file_name.clone()).unwrap();

            assert_eq!(data, "testing");
        });

        assert!(rc.is_err());
    }

    #[test]
    fn test_open_stdout() {
        let output_file_name = "-".to_string();

        let args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some("-".to_string()),
            String::from("data/all-valid-types.txt"),
        );

        let rc = panic::catch_unwind(|| {
            let mut file = open_output_file_name(&args, output_file_name.clone()).unwrap();

            file.write("testing\n".as_bytes()).unwrap();
            file.flush().unwrap();
        });

        assert!(rc.is_ok());
    }

    #[test]
    fn test_output_filename_generation_with_directory_in_original() {
        let mut args = Args::with_defaults();
        args.input = "/long/path/to/data/foo".to_string();

        let file_name = generate_output_file_name(&args, None).unwrap();
        assert_eq!(file_name, "foo.csv")
    }

    #[test]
    fn test_validate_output_file_name_ok_with_good_file_name() {
        let output_file_name = "bar.csv";

        let args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(output_file_name.to_string()),
            String::from("data/all-valid-types.txt"),
        );

        let rc = validate_output_file_name_ok(&args, &output_file_name.to_string());

        assert!(rc.is_ok());
    }

    #[test]
    fn test_validate_output_file_name_ok_with_same_name_as_input() {
        let file_name = "data/all-types.bin";

        let args = Args::with_most_defaults(
            String::from(file_name),
            Some(file_name.to_string()),
            String::from("data/all-valid-types.txt"),
        );

        let rc = validate_output_file_name_ok(&args, &file_name.to_string());

        assert!(rc.is_err());
        assert_eq!("can't overwrite input file", rc.unwrap_err().to_string());
    }

    #[test]
    fn test_validate_output_file_name_ok_with_same_name_as_types_file() {
        let file_name = "data/all-valid-types.txt";

        let args = Args::with_most_defaults(
            String::from("data/all-types.bin"),
            Some(file_name.to_string()),
            String::from(file_name),
        );

        let rc = validate_output_file_name_ok(&args, &file_name.to_string());

        assert!(rc.is_err());
        assert_eq!("can't overwrite types file", rc.unwrap_err().to_string());
    }

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
                result.err().unwrap().to_string(),
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

            let contents: Vec<Value> = f
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

            let contents: Value = serde_json::from_reader(f).unwrap();

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

    #[test]
    fn test_csv_file_max_rows() {
        let tmp_dir = temp_dir().to_str().unwrap().to_string();
        let uuid = Uuid::new_v4().to_string();

        let output_file_name = format!("{}/{}.csv", &tmp_dir, uuid);

        let mut args = Args::with_most_defaults(
            String::from("data/all-types-ten-rows.bin"),
            Some(output_file_name.clone()),
            String::from("data/all-valid-types-with-names.txt"),
        );

        args.max_rows = 5_usize;

        let rc = panic::catch_unwind(|| {
            let result = process_file(args);

            assert!(result.is_ok());

            let files = fs::read_dir(tmp_dir).unwrap();

            let files_of_iterest = files
                .map(|file| file.unwrap().file_name().into_string().unwrap())
                .filter(|file| file.starts_with(&uuid))
                .collect::<Vec<String>>();

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
}
