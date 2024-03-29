# Vertica Reader
![Build Status](https://github.com/joeygibson/verticareader/workflows/build/badge.svg)
![Release Status](https://github.com/joeygibson/verticareader/workflows/release/badge.svg)

A tool to read [Vertica native binary files](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm)
and output them in a CSV, or JSON, format. 

## Usage

```bash
A program to read Vertica native binary files and convert them to CSV, or JSON.

Usage: verticareader [OPTIONS] --types <TYPES> <INPUT>

Arguments:
  <INPUT>  The file to process

Options:
  -d, --delimiter <DELIMITER>  Field delimiter for CSV file [default: ,]
  -g, --gzip                   Compress output file using gzip
  -h, --help                   Print help information
  -H, --hex-prefix             Prefix hex strings with 0x
  -j, --json                   Output in JSON format [default: CSV]
  -J, --json-lines             Output in JSON Lines format [default: CSV]
  -l, --limit <LIMIT>          Only take the first <LIMIT> rows
  -n, --no-header              Don't include column header row in CSV file
  -o, --output <OUTPUT>        Output file name; use - for stdout [default: name based on input file name]
  -s, --single-quotes          Use ' for quoting in CSV file
  -t, --types <TYPES>          File with list of column types, names, and conversions
  -V, --version                Print version information
  -z, --tz-offset <TZ_OFFSET>  +/- hours [default: 0]
```

At its simplest, `verticareader` will read in a Vertica native file, along with a
file describing the column types and, optionally, names, and will output the file in
human-readable CSV format to standard output. Any errors will go to standard error,
so if you are planning on redirecting `stdout`, you should also redirect `stderr` with
something like `2> errs.log`. 

Using the [sample file from Vertica](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/Example.htm)
, to write to `stdout`, it can be run like this:

```bash
$ ./verticareader -t data/all-valid-types.txt data/all-types.bin
1,-1.11,one,ONE,true,1999-01-08,1999-02-23 03:11:52.350,1999-01-08 12:04:37+00,07:09:23,15:12:34-05,0xABCD,0xABCD,1234532,03:03:03

$
```

If you want to send the output to a file, instead of `stdout`, run it like this:

```bash
$ ./verticareader -t data/all-valid-types.txt -o all-types.csv data/all-types.bin
```

## Type File Format

The [Vertica native binary format](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm)
provides column sizes, but not column types, or column names. In
order to parse a binary file, the user needs to provide an additional file which contains the types
of each column, one per line. 

The names of the columns can also be provided in the same file, separated from their types
by a `/`.

A third optional value can also be provided for how to convert `varbinary` and `binary` columns. In
order to specify a column conversion, column names must also be included, with `/` separating each
value. Currently, IP Addresses (v4, and v6), and MAC addresses are supported. The possible values are 

* ipaddress
* macaddress

### Example of just types

```
Integer
Float
Char
Varchar
Boolean
Date
Timestamp
TimestampTz
Time
TimeTz
Varbinary
Binary
Numeric
Interval
```

### Example of types and names

```
Integer/IntCol
Float/FloatCol
Char / CharCol
Varchar/VarCharCol
Boolean/Bools
Date/The_Date
Timestamp/TS_Elliot
TimestampTz/TS_TZ
Time/Clock
TimeTz/Clock_TZ
Varbinary/VB3
Binary/BiN
Numeric/Num_Num_Num
Interval/Space_Between
```

### Example of types, names, and conversions

```
Integer/IntCol
Float/FloatCol
Char / CharCol
Varchar/VarCharCol
Boolean/Bools
Date/The_Date
Timestamp/TS_Elliot
TimestampTz/TS_TZ
Time/Clock
TimeTz/Clock_TZ
Varbinary/server_ip_address/ipaddress
Varbinary/server_mac_address/macaddress
Binary/BiN
Numeric/Num_Num_Num
Interval/Space_Between
```

## Accuracy

This code was tested against [the example provided by Vertica](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/Example.htm)
which is included in `data/all-types.bin`.

## Test data

I created the test data files using the Vertica-supplied [hex source](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/Example.htm).
I ran it through the script in `scripts/hex-to-binary` to get the binary version. Similarly,
I modified that hex source to generate the other two data files, with their various
changes, and ran `scripts/hex-to-binary` on them, too.

To go from binary to hex, you can run `hexdump -v <input file>`.

