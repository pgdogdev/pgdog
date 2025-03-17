use std::num::ParseIntError;

use crate::net::messages::data_row::Data;

use super::*;
use bytes::Bytes;

#[derive(Eq, PartialEq, Ord, PartialOrd, Default, Debug, Clone, Hash)]
pub struct Interval {
    years: i64,
    months: i8,
    days: i8,
    hours: i8,
    minutes: i8,
    seconds: i8,
    millis: i16,
}

impl Add for Interval {
    type Output = Interval;

    fn add(self, rhs: Self) -> Self::Output {
        // Postgres will figure it out.
        Self {
            years: self.years + rhs.years,
            months: self.months + rhs.months,
            days: self.days + rhs.days,
            hours: self.hours + rhs.hours,
            minutes: self.minutes + rhs.minutes,
            seconds: self.seconds + rhs.seconds,
            millis: self.millis + rhs.millis,
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
parser!(tinyint, i8);
parser!(smallint, i16);

impl FromDataType for Interval {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => Err(Error::NotTextEncoding),

            Format::Text => {
                let mut result = Interval::default();
                let s = String::decode(bytes, Format::Text)?;
                let mut iter = s.split(" ");
                while let Some(value) = iter.next() {
                    let format = iter.next();

                    if let Some(format) = format {
                        match format {
                            "years" => result.years = bigint(value)?,
                            "mons" => result.months = tinyint(value)?,
                            "days" => result.days = tinyint(value)?,
                            _ => (),
                        }
                    } else {
                        let mut value = value.split(":");
                        let hours = value.next();
                        if let Some(hours) = hours {
                            result.hours = tinyint(hours)?;
                        }
                        let minutes = value.next();
                        if let Some(minutes) = minutes {
                            result.minutes = tinyint(minutes)?;
                        }
                        let seconds = value.next();
                        if let Some(seconds) = seconds {
                            let mut parts = seconds.split(".");
                            let seconds = parts.next();
                            let millis = parts.next();

                            if let Some(seconds) = seconds {
                                result.seconds = tinyint(seconds)?;
                            }

                            if let Some(millis) = millis {
                                result.millis = smallint(millis)?;
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
            Format::Text => Ok(Bytes::copy_from_slice(
                format!(
                    "{} years {} mons {} days {}:{}:{}.{}",
                    self.years,
                    self.months,
                    self.days,
                    self.hours,
                    self.minutes,
                    self.seconds,
                    self.millis
                )
                .as_bytes(),
            )),
            Format::Binary => Err(Error::NotTextEncoding),
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
            millis: 500,
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
        assert_eq!(interval.millis, 6);

        let s = "00:46:12".as_bytes();
        let interval = Interval::decode(s, Format::Text).unwrap();
        assert_eq!(interval.hours, 0);
        assert_eq!(interval.minutes, 46);
        assert_eq!(interval.seconds, 12);
        assert_eq!(interval.years, 0);
    }
}
