use std::env;
use std::path::Path;

use getopts::Options;

use verticareader::process_file;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    let args: Vec<String> = env::args().collect();
    let path_to_program = Path::new(args[0].as_str());
    let program = path_to_program.file_name().unwrap().to_str().unwrap();

    let mut opts = Options::new();
    opts.optopt(
        "o",
        "output",
        "output file name (default is stdout)",
        "NAME",
    );

    opts.optopt(
        "t",
        "types",
        "file with list of column types, in order, one per line (optional names, separated by /)",
        "NAME",
    );

    opts.optopt(
        "z",
        "tz-offset",
        "offset hours for times without TZ",
        "+/-HOURS",
    );

    opts.optopt(
        "d",
        "delimiter",
        "field delimiter (default is ,)",
        "DELIMITER",
    );

    opts.optflag("s", "single-quotes", "use ' for quoting (default is \")");
    opts.optflag("h", "help", "display this help message");
    opts.optflag("v", "version", "display the program version");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("v") {
        println!("{} v{}", program, VERSION);
        return;
    }

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let output = matches.opt_str("o");

    let input = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        eprintln!("no input file given");
        print_usage(&program, opts);
        return;
    };

    let types = matches.opt_str("t");

    if types.is_none() {
        eprintln!("no column types file given");
        print_usage(&program, opts);
        return;
    }

    let tz_offset = matches.opt_str("z");
    let quote = if matches.opt_present("s") {
        b'\''
    } else {
        b'"'
    };

    let delimiter = match matches.opt_str("d") {
        None => b',',
        Some(d) => d.as_bytes()[0],
    };

    match process_file(input, output, types.unwrap(), tz_offset, quote, delimiter) {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!(
        "{} v{}\nUsage: {} FILE [options]",
        program, VERSION, program
    );

    println!("{}", opts.usage(&brief));
}
