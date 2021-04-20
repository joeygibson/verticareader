use std::env;

use clap::{App, Arg};

use verticareader::process_file;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    let app = App::new("verticareader")
        .version(VERSION)
        .about("read Vertica native binary files")
        .arg(
            Arg::with_name("input")
                .takes_value(true)
                .help("The file to process")
        )
        .arg(
            Arg::with_name("output")
                .takes_value(true)
                .short("o")
                .long("output")
                .help("Output file name [default: stdout]")
        )
        .arg(
            Arg::with_name("types")
                .takes_value(true)
                .short("t")
                .long("types")
                .help("File with list of column types, names, and conversions")
        )
        .arg(
            Arg::with_name("tz-offset")
                .takes_value(true)
                .short("z")
                .long("tz-offset")
                .help("+/- hours")
        )
        .arg(
            Arg::with_name("delimiter")
                .takes_value(true)
                .default_value(",")
                .short("d")
                .long("delimiter")
                .help("Field delimiter")
        )
        .arg(
            Arg::with_name("no-header")
                .takes_value(false)
                .short("n")
                .long("no-header")
                .help("Don't include column header row")
        )
        .arg(
            Arg::with_name("single-quotes")
                .takes_value(false)
                .short("s")
                .long("single-quotes")
                .help("Use ' for quoting")
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

    match process_file(input, output, types, tz_offset, quote, delimiter, no_header) {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }
}
