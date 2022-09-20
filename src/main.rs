use std::env;

use clap::{App, AppSettings, Arg};

use verticareader::process_file;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    let app = App::new("verticareader")
        .version(VERSION)
        .about("convert Vertica native binary files to CSV/JSON")
        .setting(AppSettings::ArgRequiredElseHelp)
        .arg(
            Arg::with_name("input")
                .takes_value(true)
                .help("The file to process"),
        )
        .arg(
            Arg::with_name("output")
                .takes_value(true)
                .short('o')
                .long("output")
                .help("Output file name [default: stdout]"),
        )
        .arg(
            Arg::with_name("types")
                .required(true)
                .takes_value(true)
                .short('t')
                .long("types")
                .help("File with list of column types, names, and conversions"),
        )
        .arg(
            Arg::with_name("tz-offset")
                .takes_value(true)
                .short('z')
                .long("tz-offset")
                .help("+/- hours"),
        )
        .arg(
            Arg::with_name("delimiter")
                .takes_value(true)
                .short('d')
                .long("delimiter")
                .help("Field delimiter for CSV file [default: ,]"),
        )
        .arg(
            Arg::with_name("no-header")
                .takes_value(false)
                .short('n')
                .long("no-header")
                .help("Don't include column header row in CSV file"),
        )
        .arg(
            Arg::with_name("single-quotes")
                .takes_value(false)
                .short('s')
                .long("single-quotes")
                .help("Use ' for quoting in CSV file"),
        )
        .arg(
            Arg::with_name("json")
                .takes_value(false)
                .short('j')
                .long("json")
                .help("Output in JSON format [default: CSV]"),
        )
        .arg(
            Arg::with_name("gzip")
                .takes_value(false)
                .short('g')
                .long("gzip")
                .help("Compress output file using gzip"),
        );

    let args = app.get_matches();

    let input = match args.value_of("input") {
        Some(filename) => String::from(filename),
        None => {
            eprintln!("no input file given\n");
            return;
        }
    };

    let output = args.value_of("output");

    let types = match args.value_of("types") {
        Some(filename) => String::from(filename),
        None => {
            eprintln!("no column types file given\n");
            return;
        }
    };

    let tz_offset = args.value_of("tz-offset");

    let quote = if args.is_present("single-quotes") {
        b'\''
    } else {
        b'"'
    };

    let delimiter = match args.value_of("delimiter") {
        None => b',',
        Some(d) => d.as_bytes()[0],
    };

    let no_header = args.is_present("no-header");
    let is_json = args.is_present("json");
    let is_gzip = args.is_present("gzip");

    match process_file(
        input, output, types, tz_offset, quote, delimiter, no_header, is_json, is_gzip,
    ) {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }
}
