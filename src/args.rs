use clap::Parser;

/// Convert Vertica native binary files to CSV/JSON
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true, next_display_order = None)]
pub struct Args {
    /// The file to process
    #[arg()]
    pub input: String,

    /// Output file name [default: stdout]
    #[arg(short, long)]
    pub output: Option<String>,

    /// File with list of column types, names, and conversions
    #[arg(short, long)]
    pub types: String,

    /// +/- hours
    #[arg(short = 'z', long, required = false, default_value_t = 0)]
    pub tz_offset: i8,

    /// Field delimiter for CSV file [default: ,]
    #[arg(short, long, default_value_t = b',', hide_default_value = true)]
    pub delimiter: u8,

    /// Don't include column header row in CSV file
    #[arg(short, long)]
    pub no_header: bool,

    /// Use ' for quoting in CSV file
    #[arg(short, long)]
    pub single_quotes: bool,

    /// Output in JSON format [default: CSV]
    #[arg(short = 'j', long = "json")]
    pub is_json: bool,

    /// Output in JSON Lines format [default: CSV]
    #[arg(short = 'J', long = "json-lines")]
    pub is_json_lines: bool,

    /// Compress output file using gzip
    #[arg(short = 'g', long = "gzip")]
    pub is_gzip: bool,

    /// Only take the first <LIMIT> rows
    #[arg(short, long, required = false, default_value_t = usize::MAX, hide_default_value=true)]
    pub limit: usize,
}
