use std::process::exit;

use clap::Parser;

use verticareader::args::Args;
use verticareader::process_file;

fn main() {
    let args = Args::parse();

    match process_file(args) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Error: {}", e);
            exit(1);
        }
    }
}
