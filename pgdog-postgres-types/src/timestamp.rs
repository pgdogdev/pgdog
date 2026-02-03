use std::fmt::Display;

use super::*;

use super::interval::bigint;
use bytes::{Buf, Bytes};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};

// PostgreSQL epoch is 2000-01-01 00:00:00 UTC, which is 946684800 seconds after Unix epoch
const POSTGRES_EPOCH_MICROS: i64 = 946684800000000; // microseconds

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Hash, Serialize, Deserialize)]
pub struct Timestamp {
    pub year: i64,
    pub month: i8,
    pub day: i8,
    pub hour: i8,
    pub minute: i8,
    pub second: i8,
    pub micros: i32,
    pub offset: Option<i8>,
    /// Special value indicator: None for normal values, Some(true) for infinity, Some(false) for -infinity
    pub special: Option<bool>,
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self.special, other.special) {
            (None, None) => self
                .year
                .cmp(&other.year)
                .then_with(|| self.month.cmp(&other.month))
                .then_with(|| self.day.cmp(&other.day))
                .then_with(|| self.hour.cmp(&other.hour))
                .then_with(|| self.minute.cmp(&other.minute))
                .then_with(|| self.second.cmp(&other.second))
                .then_with(|| self.micros.cmp(&other.micros)),
            (Some(false), _) => Ordering::Less,
            (_, Some(false)) => Ordering::Greater,
            (Some(true), _) => Ordering::Greater,
            (_, Some(true)) => Ordering::Less,
        }
    }
}

impl ToDataRowColumn for Timestamp {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.micros
        )?;

        if let Some(offset) = self.offset {
            write!(f, "{:+03}", offset)?;
        }

        Ok(())
    }
}

macro_rules! assign {
    ($result:expr, $value:tt, $parts:expr) => {
        if let Some(val) = $parts.next() {
            $result.$value = bigint(&val)?
                .try_into()
                .map_err(|_| Error::InvalidTimestamp)?;
        }
    };
}

impl Timestamp {
    /// Convert Postgres timestamp to timestamp without timezone.
    pub fn to_naive_datetime(&self) -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(self.year as i32, self.month as u32, self.day as u32)
                .unwrap_or_default(),
            NaiveTime::from_hms_micro_opt(
                self.hour as u32,
                self.minute as u32,
                self.second as u32,
                self.micros as u32,
            )
            .unwrap_or_default(),
        )
    }

    /// Create a timestamp representing positive infinity
    pub fn infinity() -> Self {
        Self {
            special: Some(true),
            ..Default::default()
        }
    }

    /// Create a timestamp representing negative infinity
    pub fn neg_infinity() -> Self {
        Self {
            special: Some(false),
            ..Default::default()
        }
    }

    /// Convert to microseconds since PostgreSQL epoch (2000-01-01)
    /// Returns i64::MAX for infinity, i64::MIN for -infinity
    pub fn to_pg_epoch_micros(&self) -> Result<i64, Error> {
        match self.special {
            Some(true) => Ok(i64::MAX),
            Some(false) => Ok(i64::MIN),
            None => {
                // Create NaiveDateTime from components
                let date =
                    NaiveDate::from_ymd_opt(self.year as i32, self.month as u32, self.day as u32)
                        .ok_or(Error::InvalidTimestamp)?;
                let time = NaiveTime::from_hms_micro_opt(
                    self.hour as u32,
                    self.minute as u32,
                    self.second as u32,
                    self.micros as u32,
                )
                .ok_or(Error::InvalidTimestamp)?;
                let dt = NaiveDateTime::new(date, time);

                // Get Unix epoch microseconds and subtract PostgreSQL epoch offset
                Ok(dt.and_utc().timestamp_micros() - POSTGRES_EPOCH_MICROS)
            }
        }
    }

    /// Create timestamp from microseconds since PostgreSQL epoch (2000-01-01)
    pub fn from_pg_epoch_micros(micros: i64) -> Result<Self, Error> {
        if micros == i64::MAX {
            return Ok(Self::infinity());
        }
        if micros == i64::MIN {
            return Ok(Self::neg_infinity());
        }

        // Convert PostgreSQL epoch to Unix epoch
        let unix_micros = micros + POSTGRES_EPOCH_MICROS;

        // Create DateTime from Unix microseconds
        let dt = chrono::DateTime::from_timestamp_micros(unix_micros)
            .ok_or(Error::InvalidTimestamp)?
            .naive_utc();

        // Extract components
        let date = dt.date();
        let time = dt.time();

        Ok(Self {
            year: date.year() as i64,
            month: date.month() as i8,
            day: date.day() as i8,
            hour: time.hour() as i8,
            minute: time.minute() as i8,
            second: time.second() as i8,
            micros: (time.nanosecond() / 1000) as i32,
            offset: None,
            special: None,
        })
    }
}

impl FromDataType for Timestamp {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, Format::Text)?;
                let mut result = Timestamp {
                    special: None,
                    ..Default::default()
                };
                let mut date_time = s.split(" ");
                let date = date_time.next();
                let time = date_time.next();

                if let Some(date) = date {
                    let mut parts = date.split("-");
                    assign!(result, year, parts);
                    assign!(result, month, parts);
                    assign!(result, day, parts);
                }

                if let Some(time) = time {
                    let mut parts = time.split(":");
                    assign!(result, hour, parts);
                    assign!(result, minute, parts);

                    if let Some(seconds) = parts.next() {
                        let mut parts = seconds.split(".");
                        assign!(result, second, parts);
                        let micros = parts.next();
                        if let Some(micros) = micros {
                            let neg = micros.find('-').is_some();
                            let mut parts = micros.split(&['-', '+']);
                            assign!(result, micros, parts);
                            if let Some(offset) = parts.next() {
                                let offset: i8 = bigint(offset)?
                                    .try_into()
                                    .map_err(|_| Error::InvalidTimestamp)?;
                                let offset = if neg { -offset } else { offset };
                                result.offset = Some(offset);
                            }
                        }
                    }
                }

                // Validate ranges
                if result.month < 1
                    || result.month > 12
                    || result.day < 1
                    || result.day > 31
                    || result.hour < 0
                    || result.hour > 23
                    || result.minute < 0
                    || result.minute > 59
                    || result.second < 0
                    || result.second > 59
                    || result.micros < 0
                    || result.micros > 999999
                {
                    return Err(Error::InvalidTimestamp);
                }

                Ok(result)
            }
            Format::Binary => {
                if bytes.len() != 8 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut bytes = bytes;
                let micros = bytes.get_i64();

                // Handle special values
                if micros == i64::MAX {
                    return Ok(Timestamp::infinity());
                }
                if micros == i64::MIN {
                    return Ok(Timestamp::neg_infinity());
                }

                // Convert microseconds to timestamp
                Timestamp::from_pg_epoch_micros(micros)
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.to_string().as_bytes())),
            Format::Binary => {
                let micros = self.to_pg_epoch_micros()?;
                Ok(Bytes::copy_from_slice(&micros.to_be_bytes()))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_timestamp() {
        let ts = "2025-03-05 14:51:42.798425".as_bytes();
        let ts = Timestamp::decode(ts, Format::Text).unwrap();

        assert_eq!(ts.year, 2025);
        assert_eq!(ts.month, 3);
        assert_eq!(ts.day, 5);
        assert_eq!(ts.hour, 14);
        assert_eq!(ts.minute, 51);
        assert_eq!(ts.second, 42);
        assert_eq!(ts.micros, 798425);
    }

    #[test]
    fn test_binary_decode_pg_epoch() {
        let bytes: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        let ts = Timestamp::decode(&bytes, Format::Binary).unwrap();

        assert_eq!(ts.year, 2000);
        assert_eq!(ts.month, 1);
        assert_eq!(ts.day, 1);
        assert_eq!(ts.hour, 0);
        assert_eq!(ts.minute, 0);
        assert_eq!(ts.second, 0);
        assert_eq!(ts.micros, 0);
    }

    #[test]
    fn test_binary_decode_specific_timestamp() {
        let bytes: [u8; 8] = [0x00, 0x02, 0xDC, 0x6C, 0x0E, 0xBC, 0xBE, 0x00];
        let result = Timestamp::decode(&bytes, Format::Binary);
        assert!(result.is_ok());
    }

    #[test]
    fn test_binary_decode_infinity() {
        let bytes: [u8; 8] = [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let ts = Timestamp::decode(&bytes, Format::Binary).unwrap();
        assert_eq!(ts.special, Some(true));
    }

    #[test]
    fn test_binary_decode_neg_infinity() {
        let bytes: [u8; 8] = [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let ts = Timestamp::decode(&bytes, Format::Binary).unwrap();
        assert_eq!(ts.special, Some(false));
    }

    #[test]
    fn test_binary_decode_wrong_size() {
        let bytes: [u8; 4] = [0, 0, 0, 0];
        let result = Timestamp::decode(&bytes, Format::Binary);
        assert!(result.is_err());

        let bytes: [u8; 12] = [0; 12];
        let result = Timestamp::decode(&bytes, Format::Binary);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_encode_pg_epoch() {
        let ts = Timestamp {
            year: 2000,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let encoded = ts.encode(Format::Binary).unwrap();
        assert_eq!(encoded.len(), 8);
        assert_eq!(&encoded[..], &[0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_binary_encode_specific_timestamp() {
        let ts = Timestamp {
            year: 2025,
            month: 7,
            day: 18,
            hour: 12,
            minute: 34,
            second: 56,
            micros: 789012,
            offset: None,
            special: None,
        };

        let encoded = ts.encode(Format::Binary).unwrap();
        assert_eq!(encoded.len(), 8);
    }

    #[test]
    fn test_binary_round_trip() {
        let original = Timestamp {
            year: 2023,
            month: 6,
            day: 15,
            hour: 14,
            minute: 30,
            second: 45,
            micros: 123456,
            offset: None,
            special: None,
        };

        let encoded = original.encode(Format::Binary).unwrap();
        let decoded = Timestamp::decode(&encoded, Format::Binary).unwrap();

        assert_eq!(decoded.year, original.year);
        assert_eq!(decoded.month, original.month);
        assert_eq!(decoded.day, original.day);
        assert_eq!(decoded.hour, original.hour);
        assert_eq!(decoded.minute, original.minute);
        assert_eq!(decoded.second, original.second);
        assert_eq!(decoded.micros, original.micros);
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp {
            year: 2020,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let ts2 = Timestamp {
            year: 2021,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        assert!(ts1 < ts2);
        assert!(ts2 > ts1);
        assert_eq!(ts1.cmp(&ts1), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_timestamp_microsecond_ordering() {
        let ts1 = Timestamp {
            year: 2020,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 100,
            offset: None,
            special: None,
        };

        let ts2 = Timestamp {
            year: 2020,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 200,
            offset: None,
            special: None,
        };

        assert!(ts1 < ts2);
    }

    #[test]
    fn test_to_pg_epoch_micros() {
        let ts = Timestamp {
            year: 2000,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };
        let micros = ts.to_pg_epoch_micros().unwrap();
        assert_eq!(micros, 0);
    }

    #[test]
    fn test_from_pg_epoch_micros() {
        let ts = Timestamp::from_pg_epoch_micros(0).unwrap();
        assert_eq!(ts.year, 2000);
        assert_eq!(ts.month, 1);
        assert_eq!(ts.day, 1);
        assert_eq!(ts.hour, 0);
        assert_eq!(ts.minute, 0);
        assert_eq!(ts.second, 0);
        assert_eq!(ts.micros, 0);
    }

    #[test]
    fn test_infinity_creation() {
        let inf = Timestamp::infinity();
        let neg_inf = Timestamp::neg_infinity();
        assert!(neg_inf < inf);
        assert_eq!(inf.special, Some(true));
        assert_eq!(neg_inf.special, Some(false));
    }

    #[test]
    fn test_invalid_timestamp_components() {
        // Test invalid date components
        let invalid_ts = Timestamp {
            year: 2025,
            month: 13, // Invalid month
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            micros: 0,
            offset: None,
            special: None,
        };

        // Should return error, not panic
        assert!(invalid_ts.to_pg_epoch_micros().is_err());

        // Test invalid day
        let invalid_ts2 = Timestamp {
            year: 2025,
            month: 2,
            day: 30, // February 30th doesn't exist
            hour: 12,
            minute: 30,
            second: 45,
            micros: 0,
            offset: None,
            special: None,
        };
        assert!(invalid_ts2.to_pg_epoch_micros().is_err());

        // Test invalid time components
        let invalid_ts3 = Timestamp {
            year: 2025,
            month: 1,
            day: 1,
            hour: 25, // Invalid hour
            minute: 30,
            second: 45,
            micros: 0,
            offset: None,
            special: None,
        };
        assert!(invalid_ts3.to_pg_epoch_micros().is_err());
    }

    #[test]
    fn test_text_parsing_validation() {
        // Test parsing invalid month
        let invalid = "2025-13-05 14:51:42.798425".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());

        // Test parsing invalid day
        let invalid = "2025-02-32 14:51:42.798425".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());

        // Test parsing invalid hour
        let invalid = "2025-03-05 25:51:42.798425".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());

        // Test parsing invalid minute
        let invalid = "2025-03-05 14:61:42.798425".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());

        // Test parsing invalid second
        let invalid = "2025-03-05 14:51:65.798425".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());

        // Test parsing invalid microseconds
        let invalid = "2025-03-05 14:51:42.9999999".as_bytes();
        let result = Timestamp::decode(invalid, Format::Text);
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_encoding_with_invalid_components() {
        // Test that binary encoding handles errors properly
        let invalid_ts = Timestamp {
            year: 2025,
            month: 13, // Invalid
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        // encode should propagate the error from to_pg_epoch_micros
        let result = invalid_ts.encode(Format::Binary);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidTimestamp));
    }

    #[test]
    fn test_from_pg_epoch_micros_special_values() {
        // Test that special values are handled correctly
        let infinity_result = Timestamp::from_pg_epoch_micros(i64::MAX);
        assert!(infinity_result.is_ok());
        assert_eq!(infinity_result.unwrap().special, Some(true));

        let neg_infinity_result = Timestamp::from_pg_epoch_micros(i64::MIN);
        assert!(neg_infinity_result.is_ok());
        assert_eq!(neg_infinity_result.unwrap().special, Some(false));
    }

    #[test]
    fn test_microsecond_parsing_still_works() {
        // This test confirms that microsecond parsing still works after removing
        // the duplicate assign! on line 199
        let ts_with_micros = "2025-03-05 14:51:42.123456".as_bytes();
        let ts = Timestamp::decode(ts_with_micros, Format::Text).unwrap();
        assert_eq!(ts.micros, 123456);

        // Test with timezone offset too
        let ts_with_tz = "2025-03-05 14:51:42.654321-08".as_bytes();
        let ts = Timestamp::decode(ts_with_tz, Format::Text).unwrap();
        assert_eq!(ts.micros, 654321);
        assert_eq!(ts.offset, Some(-8));

        // Test with positive offset
        let ts_with_tz = "2025-03-05 14:51:42.999999+05".as_bytes();
        let ts = Timestamp::decode(ts_with_tz, Format::Text).unwrap();
        assert_eq!(ts.micros, 999999);
        assert_eq!(ts.offset, Some(5));
    }

    #[test]
    fn test_datum_timestamp_comparison() {
        use super::super::Datum;

        let ts1 = Timestamp {
            year: 2020,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let ts2 = Timestamp {
            year: 2021,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let d1 = Datum::Timestamp(ts1);
        let d2 = Datum::Timestamp(ts2);

        assert!(d1 < d2);
        assert_eq!(d1.partial_cmp(&d2), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_display_offset_formatting() {
        let ts_utc = Timestamp {
            year: 2024,
            month: 1,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            micros: 123456,
            offset: Some(0),
            special: None,
        };
        assert_eq!(ts_utc.to_string(), "2024-01-15 12:30:45.123456+00");

        let ts_positive = Timestamp {
            year: 2024,
            month: 1,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            micros: 0,
            offset: Some(5),
            special: None,
        };
        assert_eq!(ts_positive.to_string(), "2024-01-15 12:30:45.000000+05");

        let ts_negative = Timestamp {
            year: 2024,
            month: 1,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            micros: 0,
            offset: Some(-8),
            special: None,
        };
        assert_eq!(ts_negative.to_string(), "2024-01-15 12:30:45.000000-08");

        let ts_no_offset = Timestamp {
            year: 2024,
            month: 1,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            micros: 0,
            offset: None,
            special: None,
        };
        assert_eq!(ts_no_offset.to_string(), "2024-01-15 12:30:45.000000");
    }

    #[test]
    fn test_datum_timestamp_encode() {
        use super::super::Datum;

        let ts = Timestamp {
            year: 2024,
            month: 6,
            day: 15,
            hour: 10,
            minute: 30,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let datum = Datum::Timestamp(ts);
        let encoded = datum.encode(Format::Text).unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "2024-06-15 10:30:00.000000"
        );
    }

    #[test]
    fn test_datum_timestamp_encode_binary() {
        use super::super::Datum;

        let ts = Timestamp {
            year: 2024,
            month: 6,
            day: 15,
            hour: 10,
            minute: 30,
            second: 0,
            micros: 0,
            offset: None,
            special: None,
        };

        let datum = Datum::Timestamp(ts.clone());
        let encoded = datum.encode(Format::Binary).unwrap();

        let decoded = Timestamp::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.year, ts.year);
        assert_eq!(decoded.month, ts.month);
        assert_eq!(decoded.day, ts.day);
        assert_eq!(decoded.hour, ts.hour);
        assert_eq!(decoded.minute, ts.minute);
        assert_eq!(decoded.second, ts.second);
        assert_eq!(decoded.micros, ts.micros);
    }
}
