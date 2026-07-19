//! PostgreSQL `oid` data type.

use super::*;
use bytes::{Buf, Bytes};
use std::fmt;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Oid(pub u32);

impl Oid {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for Oid {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<Oid> for u32 {
    fn from(value: Oid) -> Self {
        value.0
    }
}

impl FromDataType for Oid {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                let bytes: [u8; 4] = bytes.try_into()?;
                Ok(Self(bytes.as_slice().get_u32()))
            }

            Format::Text => {
                let s = String::decode(bytes, Format::Text)?;
                Ok(Self(s.parse()?))
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.0.to_string().as_bytes())),
            Format::Binary => Ok(Bytes::copy_from_slice(&self.0.to_be_bytes())),
        }
    }
}

impl ToDataRowColumn for Oid {
    fn to_data_row_column(&self) -> Data {
        Bytes::copy_from_slice(self.0.to_string().as_bytes()).into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_oid_text_round_trip_small() {
        let oid = Oid(16384); // typical user-table oid
        let encoded = oid.encode(Format::Text).unwrap();
        let decoded = Oid::decode(&encoded, Format::Text).unwrap();
        assert_eq!(oid, decoded);
    }

    #[test]
    fn test_oid_text_round_trip_above_i32_max() {
        // Regression for issue #847: pg_class.oid can exceed i32::MAX in
        // long-lived databases. Parsing as i32 used to silently produce 0.
        let oid = Oid(2_500_000_000);
        let encoded = oid.encode(Format::Text).unwrap();
        assert_eq!(&encoded[..], b"2500000000");
        let decoded = Oid::decode(&encoded, Format::Text).unwrap();
        assert_eq!(oid, decoded);
    }

    #[test]
    fn test_oid_text_round_trip_max() {
        let oid = Oid(u32::MAX);
        let encoded = oid.encode(Format::Text).unwrap();
        let decoded = Oid::decode(&encoded, Format::Text).unwrap();
        assert_eq!(oid, decoded);
    }

    #[test]
    fn test_oid_binary_round_trip() {
        for value in [0u32, 1, 16384, 2_147_483_647, 2_147_483_648, u32::MAX] {
            let oid = Oid(value);
            let encoded = oid.encode(Format::Binary).unwrap();
            assert_eq!(encoded.len(), 4);
            let decoded = Oid::decode(&encoded, Format::Binary).unwrap();
            assert_eq!(oid, decoded);
        }
    }

    #[test]
    fn test_oid_decode_text_invalid() {
        // Non-numeric text should error, not silently produce 0.
        let result = Oid::decode(b"not-a-number", Format::Text);
        assert!(matches!(result, Err(Error::NotInteger(_))));
    }

    #[test]
    fn test_oid_decode_text_negative_rejected() {
        // Negative values are not valid OIDs.
        let result = Oid::decode(b"-1", Format::Text);
        assert!(matches!(result, Err(Error::NotInteger(_))));
    }

    #[test]
    fn test_oid_decode_binary_wrong_size() {
        let result = Oid::decode(&[0u8; 3], Format::Binary);
        assert!(result.is_err());
    }

    #[test]
    fn test_oid_display() {
        assert_eq!(Oid(0).to_string(), "0");
        assert_eq!(Oid(2_500_000_000).to_string(), "2500000000");
    }

    #[test]
    fn test_oid_default_is_zero() {
        assert_eq!(Oid::default(), Oid(0));
    }
}
