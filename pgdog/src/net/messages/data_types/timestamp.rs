use std::fmt::Display;

use super::*;

use super::interval::bigint;
use bytes::{Buf, Bytes};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Hash)]
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
            "{}-{}-{} {}:{}:{}.{}",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.micros
        )?;

        if let Some(offset) = self.offset {
            write!(f, "{}{}", if offset > 0 { "+" } else { "-" }, offset)?;
        }

        Ok(())
    }
}

macro_rules! assign {
    ($result:expr, $value:tt, $parts:expr) => {
        if let Some(val) = $parts.next() {
            $result.$value = bigint(&val)?.try_into().unwrap();
        }
    };
}

impl Timestamp {
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
    pub fn to_pg_epoch_micros(&self) -> i64 {
        match self.special {
            Some(true) => i64::MAX,
            Some(false) => i64::MIN,
            None => {
                let mut days: i64 = 0;
                for year in 2000..self.year {
                    days += if is_leap_year(year) { 366 } else { 365 };
                }
                for month in 1..self.month {
                    days += days_in_month(self.year, month);
                }
                days += (self.day - 1) as i64;
                let total_micros = days * 24 * 60 * 60 * 1_000_000
                    + (self.hour as i64) * 60 * 60 * 1_000_000
                    + (self.minute as i64) * 60 * 1_000_000
                    + (self.second as i64) * 1_000_000
                    + (self.micros as i64);

                total_micros
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
        let mut remaining_micros = micros;
        let micros_in_day = 24 * 60 * 60 * 1_000_000i64;
        let days = remaining_micros / micros_in_day;
        remaining_micros %= micros_in_day;

        let micros_in_hour = 60 * 60 * 1_000_000i64;
        let hours = remaining_micros / micros_in_hour;
        remaining_micros %= micros_in_hour;

        let micros_in_minute = 60 * 1_000_000i64;
        let minutes = remaining_micros / micros_in_minute;
        remaining_micros %= micros_in_minute;

        let seconds = remaining_micros / 1_000_000;
        let microseconds = remaining_micros % 1_000_000;
        let (year, month, day) = days_to_date(2000, days);

        Ok(Self {
            year,
            month: month as i8,
            day: day as i8,
            hour: hours as i8,
            minute: minutes as i8,
            second: seconds as i8,
            micros: microseconds as i32,
            offset: None,
            special: None,
        })
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i64, month: i8) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn days_to_date(base_year: i64, days: i64) -> (i64, u32, u32) {
    let mut year = base_year;
    let mut remaining_days = days;

    // Find the year
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }
    let mut month = 1;
    while month <= 12 {
        let days_in_this_month = days_in_month(year, month);
        if remaining_days < days_in_this_month {
            break;
        }
        remaining_days -= days_in_this_month;
        month += 1;
    }

    let day = remaining_days + 1;

    (year, month as u32, day as u32)
}

impl FromDataType for Timestamp {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, Format::Text)?;
                let mut result = Timestamp::default();
                result.special = None; // Ensure text timestamps are normal values
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
                                let offset: i8 = bigint(offset)?.try_into().unwrap();
                                let offset = if neg { -offset } else { offset };
                                result.offset = Some(offset);
                            }
                        }
                        assign!(result, micros, parts);
                    }
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
                let micros = self.to_pg_epoch_micros();
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
        let micros = ts.to_pg_epoch_micros();
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
}
