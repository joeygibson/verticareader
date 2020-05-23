use std::env;

use getopts::Options;

use verticareader::process_file;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt(
        "o",
        "output",
        "output file name (defaults to stdout)",
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
    opts.optflag("h", "help", "display this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

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

    match process_file(input, output, types.unwrap(), tz_offset) {
        Ok(_) => {}
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);

    println!("{}", opts.usage(&brief));
}
