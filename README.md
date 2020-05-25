# Vertica Reader

A tool to read [Vertica native binary files](https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm)
and output them in a CSV format. 

## Usage

```bash
Usage: verticareader FILE [options]

Options:
    -o, --output NAME   output file name (defaults to stdout)
    -t, --types NAME    file with list of column types, in order, one per line (optional names, separated by /)
    -z, --tz-offset +/-HOURS
                        offset hours for times without TZ
    -h, --help          display this help message
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
