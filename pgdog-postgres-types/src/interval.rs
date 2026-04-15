use std::{num::ParseIntError, ops::Add};

use crate::Data;

use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Eq, PartialEq, Ord, PartialOrd, Default, Debug, Clone, Hash)]
pub struct Interval {
    years: i64,
    months: i32,
    days: i32,
    hours: i64,
    minutes: i64,
    seconds: i64,
    micros: i32,
}

impl Add for Interval {
    type Output = Interval;

    fn add(self, rhs: Self) -> Self::Output {
        // Postgres will figure it out.
        Self {
            years: self.years.saturating_add(rhs.years),
            months: self.months.saturating_add(rhs.months),
            days: self.days.saturating_add(rhs.days),
            hours: self.hours.saturating_add(rhs.hours),
            minutes: self.minutes.saturating_add(rhs.minutes),
            seconds: self.seconds.saturating_add(rhs.seconds),
            micros: self.micros.saturating_add(rhs.micros),
        }
    }
}

impl ToDataRowColumn for Interval {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

macro_rules! parser {
    ($name:tt, $typ:ty) => {
        pub(super) fn $name(s: &str) -> Result<$typ, ParseIntError> {
            // Skip leading zeros.
            let mut cnt = 0;
            for c in s.chars() {
                if c == '0' {
                    cnt += 1;
                } else {
                    break;
                }
            }

            let slice = &s[cnt..];
            if slice.is_empty() {
                Ok(0)
            } else {
                s[cnt..].parse()
            }
        }
    };
}

parser!(bigint, i64);
parser!(int32, i32);

fn parse_fractional_micros(s: &str) -> Result<i32, Error> {
    if s.is_empty() || s.len() > 6 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::UnexpectedPayload);
    }

    let micros = s.parse::<i32>()?;
    Ok(micros * 10_i32.pow(6 - s.len() as u32))
}

fn format_fractional_micros(micros: i32) -> String {
    if micros == 0 {
        return "0".into();
    }

    let mut digits = format!("{:06}", micros.abs());
    while digits.ends_with('0') {
        digits.pop();
    }
    digits
}

impl FromDataType for Interval {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                // PostgreSQL binary interval: microseconds(i64) + days(i32) + months(i32) = 16 bytes
                if bytes.len() != 16 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }
                let mut buf = bytes;
                let microseconds = buf.get_i64();
                let days = buf.get_i32();
                let total_months = buf.get_i32();

                let years = (total_months / 12) as i64;
                let months = total_months % 12;

                let total_secs = microseconds / 1_000_000;
                let remaining_micros = microseconds % 1_000_000;
                let hours = total_secs / 3600;
                let minutes = (total_secs % 3600) / 60;
                let seconds = total_secs % 60;
                let micros = remaining_micros as i32;

                Ok(Self {
                    years,
                    months,
                    days,
                    hours,
                    minutes,
                    seconds,
                    micros,
                })
            }

            Format::Text => {
                let mut result = Interval::default();
                let s = String::decode(bytes, Format::Text)?;
                let mut iter = s.split(" ");
                while let Some(value) = iter.next() {
                    let format = iter.next();

                    if let Some(format) = format {
                        match format {
                            "year" | "years" => result.years = bigint(value)?,
                            "mon" | "mons" => result.months = int32(value)?,
                            "day" | "days" => result.days = int32(value)?,
                            _ => (),
                        }
                    } else {
                        let mut value = value.split(":");
                        let hours = value.next();
                        if let Some(hours) = hours {
                            result.hours = bigint(hours)?;
                        }
                        let minutes = value.next();
                        if let Some(minutes) = minutes {
                            result.minutes = bigint(minutes)?;
                        }
                        let seconds = value.next();
                        if let Some(seconds) = seconds {
                            let mut parts = seconds.split(".");
                            let seconds = parts.next();
                            let millis = parts.next();

                            if let Some(seconds) = seconds {
                                result.seconds = bigint(seconds)?;
                            }

                            if let Some(millis) = millis {
                                result.micros = parse_fractional_micros(millis)?;
                            }
                        }
                    }
                }

                Ok(result)
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => {
                let years_label = if self.years == 1 { "year" } else { "years" };
                let days_label = if self.days == 1 { "day" } else { "days" };

                // Collapse time fields into a signed total so a negative fractional
                // component produces a single leading `-` on the time portion rather
                // than an unparseable `0:0:0.-5`.
                let total_micros: i64 = self
                    .hours
                    .saturating_mul(3_600_000_000)
                    .saturating_add(self.minutes.saturating_mul(60_000_000))
                    .saturating_add(self.seconds.saturating_mul(1_000_000))
                    .saturating_add(self.micros as i64);
                let negative = total_micros < 0;
                let abs_total = total_micros.unsigned_abs();
                let total_secs = (abs_total / 1_000_000) as i64;
                let remaining_micros = (abs_total % 1_000_000) as i32;
                let hours = total_secs / 3600;
                let minutes = (total_secs % 3600) / 60;
                let seconds = total_secs % 60;
                let sign = if negative { "-" } else { "" };

                Ok(Bytes::copy_from_slice(
                    format!(
                        "{} {} {} mons {} {} {}{:02}:{:02}:{:02}.{}",
                        self.years,
                        years_label,
                        self.months,
                        self.days,
                        days_label,
                        sign,
                        hours,
                        minutes,
                        seconds,
                        format_fractional_micros(remaining_micros)
                    )
                    .as_bytes(),
                ))
            }
            Format::Binary => {
                let mut buf = BytesMut::with_capacity(16);
                let microseconds = self.hours * 3_600_000_000
                    + self.minutes * 60_000_000
                    + self.seconds * 1_000_000
                    + self.micros as i64;
                let total_months = self.years as i32 * 12 + self.months;
                buf.put_i64(microseconds);
                buf.put_i32(self.days);
                buf.put_i32(total_months);
                Ok(buf.freeze())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_interval_ord() {
        let one = Interval {
            months: 2,
            seconds: 59,
            ..Default::default()
        };
        let two = Interval {
            years: 1,
            micros: 500_000,
            ..Default::default()
        };

        assert!(one < two);
    }

    #[test]
    fn test_interval_decode() {
        let s = "115 years 2 mons 19 days 16:48:00.006";
        let interval = Interval::decode(s.as_bytes(), Format::Text).unwrap();
        assert_eq!(interval.years, 115);
        assert_eq!(interval.months, 2);
        assert_eq!(interval.days, 19);
        assert_eq!(interval.hours, 16);
        assert_eq!(interval.minutes, 48);
        assert_eq!(interval.seconds, 0);
        assert_eq!(interval.micros, 6_000);

        let s = "00:46:12".as_bytes();
        let interval = Interval::decode(s, Format::Text).unwrap();
        assert_eq!(interval.hours, 0);
        assert_eq!(interval.minutes, 46);
        assert_eq!(interval.seconds, 12);
        assert_eq!(interval.years, 0);
    }

    #[test]
    fn test_interval_binary_roundtrip_preserves_fractional_seconds() {
        let cases = [
            (
                "00:00:00.006",
                0,
                6_000,
                "0 years 0 mons 0 days 00:00:00.006",
            ),
            ("00:00:06.7", 6, 700_000, "0 years 0 mons 0 days 00:00:06.7"),
        ];

        for (input, expected_seconds, expected_micros, expected_text) in cases {
            let original = Interval::decode(input.as_bytes(), Format::Text).unwrap();
            let binary = original.encode(Format::Binary).unwrap();
            let decoded = Interval::decode(&binary, Format::Binary).unwrap();

            assert_eq!(decoded.seconds, expected_seconds, "seconds for {input}");
            assert_eq!(decoded.micros, expected_micros, "micros for {input}");
            assert_eq!(
                decoded.encode(Format::Text).unwrap(),
                Bytes::from(expected_text)
            );
        }
    }

    #[test]
    fn test_interval_large_values_text_roundtrip() {
        let s = "0 years 0 mons 200 days 500:30:45.0";
        let interval = Interval::decode(s.as_bytes(), Format::Text).unwrap();
        assert_eq!(interval.days, 200);
        assert_eq!(interval.hours, 500);
        assert_eq!(interval.minutes, 30);
        assert_eq!(interval.seconds, 45);
        let encoded = interval.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], s.as_bytes());
    }

    #[test]
    fn test_interval_large_values_binary_roundtrip() {
        let original = Interval {
            years: 0,
            months: 0,
            days: 200,
            hours: 500,
            minutes: 30,
            seconds: 45,
            micros: 0,
        };
        let binary = original.encode(Format::Binary).unwrap();
        let decoded = Interval::decode(&binary, Format::Binary).unwrap();
        assert_eq!(decoded.days, 200);
        assert_eq!(decoded.hours, 500);
        assert_eq!(decoded.minutes, 30);
        assert_eq!(decoded.seconds, 45);
    }

    #[test]
    fn test_parse_fractional_micros_edge_cases() {
        assert_eq!(parse_fractional_micros("5").unwrap(), 500_000);
        assert_eq!(parse_fractional_micros("123456").unwrap(), 123_456);
        assert_eq!(parse_fractional_micros("000000").unwrap(), 0);
        assert!(parse_fractional_micros("1234567").is_err());
        assert!(parse_fractional_micros("").is_err());
        assert!(parse_fractional_micros("12a4").is_err());
    }

    #[test]
    fn test_interval_decode_postgres_style_singular_units_and_padding() {
        let s = "1 year 2 mons 1 day 04:05:06.7";
        let interval = Interval::decode(s.as_bytes(), Format::Text).unwrap();

        assert_eq!(interval.years, 1);
        assert_eq!(interval.months, 2);
        assert_eq!(interval.days, 1);
        assert_eq!(interval.hours, 4);
        assert_eq!(interval.minutes, 5);
        assert_eq!(interval.seconds, 6);
        assert_eq!(interval.micros, 700_000);

        let encoded = interval.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], s.as_bytes());
    }

    #[test]
    fn test_negative_fractional_interval_binary_text_output_is_self_parseable() {
        let original = Interval {
            micros: -500_000,
            ..Default::default()
        };

        let binary = original.encode(Format::Binary).unwrap();
        let decoded = Interval::decode(&binary, Format::Binary).unwrap();
        let text = decoded.encode(Format::Text).unwrap();

        assert!(
            Interval::decode(&text, Format::Text).is_ok(),
            "binary->text interval output should remain parseable: {}",
            std::str::from_utf8(&text).unwrap()
        );
    }
}
