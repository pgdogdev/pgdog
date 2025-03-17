use std::ops::{Deref, DerefMut};

use super::*;

#[derive(Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Default, Hash)]
pub struct TimestampTz {
    timestamp: Timestamp,
}

impl FromDataType for TimestampTz {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let timestamp = Timestamp::decode(bytes, encoding)?;
        if timestamp.offset.is_none() {
            return Err(Error::NotTimestampTz);
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
    use super::*;

    #[test]
    fn test_timestamptz() {
        let ts = "2025-03-05 14:55:02.436109-08".as_bytes();
        let ts = TimestampTz::decode(ts, Format::Text).unwrap();

        assert_eq!(ts.year, 2025);
        assert_eq!(ts.month, 3);
        assert_eq!(ts.day, 5);
        assert_eq!(ts.hour, 14);
        assert_eq!(ts.minute, 55);
        assert_eq!(ts.second, 2);
        assert_eq!(ts.micros, 436109);
        assert_eq!(ts.offset, Some(-8));
    }
}
