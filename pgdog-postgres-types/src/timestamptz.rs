use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(
    Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Default, Hash, Serialize, Deserialize,
)]
pub struct TimestampTz {
    timestamp: Timestamp,
}

impl FromDataType for TimestampTz {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let mut timestamp = Timestamp::decode(bytes, encoding)?;
        if timestamp.offset.is_none() {
            timestamp.offset = Some(0);
        }
        Ok(Self { timestamp })
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        Timestamp::encode(self, encoding)
    }
}

impl Deref for TimestampTz {
    type Target = Timestamp;

    fn deref(&self) -> &Self::Target {
        &self.timestamp
    }
}

impl DerefMut for TimestampTz {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.timestamp
    }
}

impl ToDataRowColumn for TimestampTz {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

#[cfg(test)]
mod test {
    use chrono::{Datelike, Timelike};

    use super::*;

    #[test]
    fn test_timestamptz() {
        let ts = "2025-03-05 14:55:02.436109-08".as_bytes();
        let ts = TimestampTz::decode(ts, Format::Text).unwrap();
        let time = ts.to_naive_datetime();

        assert_eq!(ts.year, 2025);
        assert_eq!(ts.month, 3);
        assert_eq!(ts.day, 5);
        assert_eq!(ts.hour, 14);
        assert_eq!(ts.minute, 55);
        assert_eq!(ts.second, 2);
        assert_eq!(ts.micros, 436109);
        assert_eq!(ts.offset, Some(-8));

        assert_eq!(time.date().year(), 2025);
        assert_eq!(time.date().month(), 3);
        assert_eq!(time.date().day(), 5);
        assert_eq!(time.time().hour(), 14);
        assert_eq!(time.time().minute(), 55);
        assert_eq!(time.time().second(), 2);
    }

    #[test]
    fn test_timestamptz_without_offset_defaults_to_utc() {
        let ts = "2024-01-15 12:30:45.123456".as_bytes();
        let ts = TimestampTz::decode(ts, Format::Text).unwrap();

        assert_eq!(ts.year, 2024);
        assert_eq!(ts.month, 1);
        assert_eq!(ts.day, 15);
        assert_eq!(ts.offset, Some(0));
    }

    #[test]
    fn test_timestamptz_encode_includes_offset() {
        let ts = "2024-01-15 12:30:45.000000".as_bytes();
        let ts = TimestampTz::decode(ts, Format::Text).unwrap();

        let encoded = ts.encode(Format::Text).unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "2024-01-15 12:30:45.000000+00"
        );
    }

    #[test]
    fn test_datum_timestamptz_encode() {
        use super::Datum;

        let ts = "2024-06-15 10:30:00.000000+05".as_bytes();
        let ts = TimestampTz::decode(ts, Format::Text).unwrap();

        let datum = Datum::TimestampTz(ts);
        let encoded = datum.encode(Format::Text).unwrap();
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "2024-06-15 10:30:00.000000+05"
        );
    }

    #[test]
    fn test_datum_timestamptz_encode_binary() {
        use super::Datum;

        let ts = "2024-06-15 10:30:00.123456+00".as_bytes();
        let original = TimestampTz::decode(ts, Format::Text).unwrap();

        let datum = Datum::TimestampTz(original);
        let encoded = datum.encode(Format::Binary).unwrap();

        let decoded = TimestampTz::decode(&encoded, Format::Binary).unwrap();
        assert_eq!(decoded.year, 2024);
        assert_eq!(decoded.month, 6);
        assert_eq!(decoded.day, 15);
        assert_eq!(decoded.hour, 10);
        assert_eq!(decoded.minute, 30);
        assert_eq!(decoded.second, 0);
        assert_eq!(decoded.micros, 123456);
    }
}
