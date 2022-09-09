use std::convert::TryInto;
use std::ops::Add;
use std::panic;
use std::result::Result;

use chrono::prelude::*;
use chrono::Duration;
use regex;
use regex::Regex;

use lazy_static::lazy_static;

use crate::column_conversion::ColumnConversion;

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Integer,
    Float,
    Char,
    Varchar,
    Boolean,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Varbinary,
    Binary,
    Numeric,
    Interval,
}

impl ColumnType {
    pub fn from_string(string: &str) -> Result<ColumnType, String> {
        lazy_static! {
            static ref PAREN_REGEX: Regex = Regex::new(r"\(.+\)").unwrap();
        }

        let no_parens = PAREN_REGEX.replace(string, "");

        let result = match no_parens.to_lowercase().as_str() {
            "integer" | "int" => ColumnType::Integer,
            "float" => ColumnType::Float,
            "char" => ColumnType::Char,
            "varchar" => ColumnType::Varchar,
            "boolean" => ColumnType::Boolean,
            "date" => ColumnType::Date,
            "timestamp" => ColumnType::Timestamp,
            "timestamptz" => ColumnType::TimestampTz,
            "time" => ColumnType::Time,
            "timetz" => ColumnType::TimeTz,
            "varbinary" => ColumnType::Varbinary,
            "binary" => ColumnType::Binary,
            "numeric" => ColumnType::Numeric,
            "interval" => ColumnType::Interval,
            _ => return Err(format!("invalid type: {}", string.clone())),
        };

        Ok(result)
    }

    pub fn format_value(
        &self,
        value: &Option<Vec<u8>>,
        tz_offset: i8,
        column_conversion: &Option<ColumnConversion>,
    ) -> String {
        match &value {
            Some(value) => {
                match &*self {
                    ColumnType::Integer => {
                        let bytes = &value[..];

                        match bytes.len() {
                            8 => format!("{}", i64::from_le_bytes(bytes.try_into().unwrap())),
                            4 => format!("{}", i32::from_le_bytes(bytes.try_into().unwrap())),
                            2 => format!("{}", i16::from_le_bytes(bytes.try_into().unwrap())),
                            1 => format!("{}", i8::from_le_bytes(bytes.try_into().unwrap())),
                            _ => panic!("incorrect integer byte count"),
                        }
                    }
                    ColumnType::Float => {
                        let bytes = &value[..];
                        format!("{}", f64::from_le_bytes(bytes.try_into().unwrap()))
                    }
                    ColumnType::Char | ColumnType::Varchar => {
                        let char_str = match std::str::from_utf8(&value) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("couldn't convert {:X?} to a string: {}", &value, e);
                                "INVALID"
                            }
                        };

                        format!("{}", char_str.trim())
                    }
                    ColumnType::Boolean => format!("{}", value[0]),
                    ColumnType::Date => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1);
                        let d = Duration::days(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);
                        format!("{}", new_date)
                    }
                    ColumnType::Timestamp => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);

                        let d = Duration::microseconds(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);
                        format!("{}", new_date)
                    }
                    ColumnType::TimestampTz => {
                        let bytes = &value[..];
                        let julian_date_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;
                        let vertica_epoch_date =
                            NaiveDate::from_ymd(2000, 1, 1).and_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(julian_date_offset);
                        let new_date = vertica_epoch_date.add(d);

                        let tz_offset_date = if tz_offset != 0 {
                            let tz_offset_hours = Duration::hours(tz_offset as i64);
                            new_date.add(tz_offset_hours)
                        } else {
                            new_date
                        };

                        let formatted_tz_offset = format!("{:+03}", tz_offset);
                        let formatted_date =
                            format!("{}{}", tz_offset_date.format("%F %T"), formatted_tz_offset);
                        format!("{}", formatted_date)
                    }
                    ColumnType::Time => {
                        let bytes = &value[..];
                        let microsecond_offset =
                            u64::from_le_bytes(bytes.try_into().unwrap()) as i64;

                        let midnight = NaiveTime::from_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(microsecond_offset);
                        let new_time = midnight.add(d);

                        format!("{}", new_time)
                    }
                    ColumnType::TimeTz => {
                        let bytes = &value[..];
                        let as_u64 = u64::from_le_bytes(bytes.try_into().unwrap());

                        let microsecond_offset: u64 = as_u64 >> 24;
                        let tz_offset_from_column: i64 = (as_u64 & 0xFFFFFF) as i64;

                        let new_offset = -((tz_offset_from_column / 3600) - 24);
                        let midnight = NaiveTime::from_hms_micro(0, 0, 0, 0);

                        let d = Duration::microseconds(microsecond_offset as i64);
                        let new_time = midnight.add(d);

                        // if we leave this as minutes, then we can handle timezones that
                        // don't align to an hour boundary
                        let tz_offset_hours = Duration::hours(new_offset as i64);
                        let offset_time = new_time.add(tz_offset_hours);

                        let formatted_tz_offset = format!("{:+03}", new_offset);
                        let formatted_date =
                            format!("{}{}", offset_time.format("%T"), formatted_tz_offset);
                        format!("{}", formatted_date)
                    }
                    ColumnType::Varbinary | ColumnType::Binary => {
                        let bytes = &value[..];
                        let filtered_bytes = bytes[..]
                            .iter()
                            .filter(|&b| *b != 0x00u8)
                            .map(|b| *b)
                            .collect::<Vec<u8>>();

                        match column_conversion {
                            None => {
                                let byte_values: String =
                                    filtered_bytes.iter().map(|b| format!("{:X?}", b)).collect();

                                format!("0x{}", byte_values)
                            }
                            Some(conversion) => conversion.convert(filtered_bytes),
                        }
                    }
                    ColumnType::Numeric => {
                        let bytes = &value[..];
                        let mut chunks: Vec<u64> = vec![];

                        for i in 0..(bytes.len() / 8) {
                            let chunk = u64::from_le_bytes(
                                bytes[(i * 8)..((i + 1) * 8)].try_into().unwrap(),
                            );

                            chunks.push(chunk);
                        }

                        let filtered_chunks: Vec<String> = chunks
                            .iter()
                            .skip_while(|chunk| **chunk == 0)
                            .map(|chunk| chunk.to_string())
                            .collect();

                        filtered_chunks.join("")
                    }
                    ColumnType::Interval => {
                        let bytes = &value[..];
                        let interval_microseconds = i64::from_le_bytes(bytes.try_into().unwrap());

                        let seconds = interval_microseconds / 1_000_000;
                        let (hours, remainder) = ((seconds / 3600), (seconds % 3600));
                        let (minutes, remainder) = ((remainder / 60), (remainder % 60));

                        format!("{:02}:{:02}:{:02}", hours, minutes, remainder)
                    }
                }
            }
            _ => "".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    mod column_type_tests {
        use crate::column_type::ColumnType;

        #[test]
        fn test_good_input() {
            let exp = ColumnType::Varbinary;
            let val = ColumnType::from_string("Varbinary").unwrap();

            assert_eq!(exp, val);
        }

        #[test]
        fn test_with_mixed_case() {
            let exp = ColumnType::Varbinary;
            let val = ColumnType::from_string("VarBiNarY").unwrap();

            assert_eq!(exp, val);
        }

        #[test]
        fn test_invalid_input() {
            let val = ColumnType::from_string("ljshdflkd");

            assert!(val.is_err(), "should not have returned valid enum");
        }
    }

    mod format_tests {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

        use crate::column_type::ColumnType;

        #[test]
        fn test_i8() {
            let column_type = ColumnType::Integer;

            let mut inputs: Vec<u8> = vec_i_into_u::<i8, u8>(vec![-127i8, -100i8, -1i8]);

            let mut positives: Vec<u8> = vec![0, 23, 127];
            inputs.append(&mut positives);

            let expected_outputs = vec!["-127", "-100", "-1", "0", "23", "127"];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec_option: Option<Vec<u8>> = Some(vec![*input]);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_i16() {
            let column_type = ColumnType::Integer;

            let mut inputs = vec_i_into_u::<i16, u16>(vec![-32768, -16600, -1]);
            let mut positives: Vec<u16> = vec![0, 23, 127, 128, 255, 256, 512, 1024, 16235];

            inputs.append(&mut positives);

            let expected_outputs = vec![
                "-32768", "-16600", "-1", "0", "23", "127", "128", "255", "256", "512", "1024",
                "16235",
            ];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_i32() {
            let column_type = ColumnType::Integer;

            let mut inputs: Vec<u32> =
                vec_i_into_u::<i32, u32>(vec![-2147483648, -101010101, -65536, -12345, -1]);

            let mut positives: Vec<u32> = vec![
                0, 23, 127, 128, 255, 256, 512, 1024, 16235, 65535, 65536, 123456, 900000,
                2147483647,
            ];

            inputs.append(&mut positives);

            let expected_outputs = vec![
                "-2147483648",
                "-101010101",
                "-65536",
                "-12345",
                "-1",
                "0",
                "23",
                "127",
                "128",
                "255",
                "256",
                "512",
                "1024",
                "16235",
                "65535",
                "65536",
                "123456",
                "900000",
                "2147483647",
            ];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_i64() {
            let column_type = ColumnType::Integer;

            let mut inputs: Vec<u64> = vec_i_into_u::<i64, u64>(vec![
                -9223372036854775808,
                -2147483648,
                -101010101,
                -65536,
                -12345,
                -1,
            ]);

            let mut positives: Vec<u64> = vec![
                0,
                23,
                127,
                128,
                255,
                256,
                512,
                1024,
                16235,
                65535,
                65536,
                123456,
                900000,
                2147483647,
                9223372036854775807,
            ];

            inputs.append(&mut positives);

            let expected_outputs = vec![
                "-9223372036854775808",
                "-2147483648",
                "-101010101",
                "-65536",
                "-12345",
                "-1",
                "0",
                "23",
                "127",
                "128",
                "255",
                "256",
                "512",
                "1024",
                "16235",
                "65535",
                "65536",
                "123456",
                "900000",
                "2147483647",
                "9223372036854775807",
            ];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_float() {
            let column_type = ColumnType::Float;

            let inputs: Vec<f64> = vec![-123456.123, -23.123, 0_f64, 123.23, 123456.123];

            let expected_outputs = vec!["-123456.123", "-23.123", "0", "123.23", "123456.123"];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_char() {
            let column_type = ColumnType::Char;

            let inputs: Vec<u8> = vec!['a' as u8, 'A' as u8, 'z' as u8, 'Z' as u8];

            let expected_outputs = vec!["a", "A", "z", "Z"];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec_option: Option<Vec<u8>> = Some(vec![*input]);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_varchar() {
            let column_type = ColumnType::Varchar;

            let inputs: Vec<&str> = vec!["a", "A", "z", "Z", "abc", "FOO", "🚀", "foo, bar, baz"];

            let expected_outputs = vec!["a", "A", "z", "Z", "abc", "FOO", "🚀", "foo, bar, baz"];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let bytes = input.as_bytes();
                let byte_vec_option: Option<Vec<u8>> = Some(bytes.to_vec());

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_boolean() {
            let column_type = ColumnType::Boolean;

            let inputs: Vec<u8> = vec![1, 0];

            let expected_outputs = vec!["1", "0"];

            for (input, expected_output) in inputs.iter().zip(expected_outputs) {
                let byte_vec_option: Option<Vec<u8>> = Some(vec![*input]);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_date() {
            let column_type = ColumnType::Date;

            let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1);

            let expected_outputs = vec!["2001-01-01", "2006-08-23", "1990-05-01"];
            let inputs: Vec<i64> = expected_outputs
                .iter()
                .map(|date_str| {
                    let date = NaiveDate::parse_from_str(*date_str, "%Y-%m-%d").unwrap();
                    let days_between = date - vertica_epoch_date;
                    days_between.num_days()
                })
                .collect();

            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_timestamp() {
            let column_type = ColumnType::Timestamp;

            let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1).and_hms_nano(0, 0, 0, 0);

            let expected_outputs = vec![
                "2001-01-01 00:00:00",
                "2006-08-23 00:00:00",
                "1990-05-01 00:00:00",
                "1980-12-25 01:23:34",
                "1492-04-05 12:12:12",
            ];
            let inputs: Vec<i64> = expected_outputs
                .iter()
                .map(|date_str| {
                    let date = match NaiveDateTime::parse_from_str(*date_str, "%Y-%m-%d %H:%M:%S") {
                        Ok(d) => d,
                        Err(e) => panic!("{}", e),
                    };

                    let diff = date.signed_duration_since(vertica_epoch_date);
                    match diff.num_microseconds() {
                        None => panic!("no microseconds"),
                        Some(micros) => micros,
                    }
                })
                .collect();

            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        #[test]
        fn test_timestamptz() {
            let column_type = ColumnType::TimestampTz;

            let vertica_epoch_date = NaiveDate::from_ymd(2000, 1, 1).and_hms_nano(0, 0, 0, 0);

            let string_inputs: Vec<&str> = vec![
                "2001-01-01 00:00:00+0000",
                "2006-08-23 00:00:00+0000",
                "1990-05-01 00:00:00+0000",
                "1980-12-25 01:23:34+0000",
                "1492-04-05 12:12:12+0000",
            ];

            let expected_outputs: Vec<String> = string_inputs
                .iter()
                .map(|s| s[0..(s.len() - 2)].to_string())
                .collect();

            let inputs: Vec<i64> = string_inputs
                .iter()
                .map(|date_str| {
                    let date = match NaiveDateTime::parse_from_str(*date_str, "%Y-%m-%d %H:%M:%S%z")
                    {
                        Ok(d) => d,
                        Err(e) => panic!("{}", e),
                    };

                    let diff = date.signed_duration_since(vertica_epoch_date);
                    match diff.num_microseconds() {
                        None => panic!("no microseconds"),
                        Some(micros) => micros,
                    }
                })
                .collect();

            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(output, expected_output);
            }
        }

        #[test]
        fn test_time() {
            let column_type = ColumnType::Time;
            let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);

            let expected_outputs = vec!["05:30:15", "11:22:33", "17:15:16"];
            let inputs: Vec<i64> = expected_outputs
                .iter()
                .map(|time_str| {
                    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S").unwrap();
                    let diff = time - midnight;
                    diff.num_microseconds().unwrap()
                })
                .collect();

            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        // TODO: I need to think more about how to test this one. It does work
        // correctly against `data/all-types.bin`.
        // #[test]
        // fn test_timetz() {
        //     let column_type = ColumnType::TimeTz;
        //     let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);
        //
        //     let expected_outputs = vec!["05:30:15", "11:22:33", "17:15:16"];
        //     let inputs: Vec<i64> = expected_outputs
        //         .iter()
        //         .map(|time_str| {
        //             let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S").unwrap();
        //             let diff = time - midnight;
        //             diff.num_microseconds().unwrap()
        //         })
        //         .collect();
        //
        //     let u_inputs = vec_i_into_u::<i64, u64>(inputs);
        //
        //     for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
        //         let byte_vec = input.to_le_bytes().to_vec();
        //         let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);
        //
        //         let output = column_type.format_value(&byte_vec_option, 0, &None);
        //
        //         assert_eq!(expected_output, output);
        //     }
        // }

        #[test]
        fn test_binary() {
            let column_type = ColumnType::Binary;

            let inputs: Vec<i64> = vec![1, 10, 123, 808080];
            let expected_outputs = vec!["0x1", "0xA", "0x7B", "0x9054C"];
            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(output, expected_output);
            }
        }

        #[test]
        fn test_numeric() {
            let column_type = ColumnType::Numeric;

            let inputs: Vec<i64> = vec![123456789, 123456789123456789];
            let expected_outputs = vec!["123456789", "123456789123456789"];
            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(output, expected_output);
            }
        }

        #[test]
        fn test_interval() {
            let column_type = ColumnType::Interval;

            let midnight = NaiveTime::from_hms_nano(0, 0, 0, 0);

            let expected_outputs = vec!["05:30:15", "11:22:33", "17:15:16"];
            let inputs: Vec<i64> = expected_outputs
                .iter()
                .map(|time_str| {
                    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S").unwrap();
                    let diff = time - midnight;
                    diff.num_microseconds().unwrap()
                })
                .collect();

            let u_inputs = vec_i_into_u::<i64, u64>(inputs);

            for (input, expected_output) in u_inputs.iter().zip(expected_outputs) {
                let byte_vec = input.to_le_bytes().to_vec();
                let byte_vec_option: Option<Vec<u8>> = Some(byte_vec);

                let output = column_type.format_value(&byte_vec_option, 0, &None);

                assert_eq!(expected_output, output);
            }
        }

        fn vec_i_into_u<T, U>(v: Vec<T>) -> Vec<U> {
            // Stolen from https://stackoverflow.com/a/59707887
            // and adapted to be generic
            // ideally we'd use Vec::into_raw_parts, but it's unstable,
            // so we have to do it manually:

            // first, make sure v's destructor doesn't free the data
            // it thinks it owns when it goes out of scope
            let mut v = std::mem::ManuallyDrop::new(v);

            // then, pick apart the existing Vec
            let p = v.as_mut_ptr();
            let len = v.len();
            let cap = v.capacity();

            // finally, adopt the data into a new Vec
            unsafe { Vec::from_raw_parts(p as *mut U, len, cap) }
        }
    }
}
