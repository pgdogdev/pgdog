use bytes::{Buf, BufMut, BytesMut};
use std::cmp::Ordering;

use crate::net::messages::data_row::Data;

use super::*;

/// Wrapper type for f64 that implements Ord for PostgreSQL compatibility
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Double(pub f64);

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        // PostgreSQL ordering: NaN is greater than all other values
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal),
        }
    }
}

impl Eq for Double {}

impl FromDataType for Double {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.to_uppercase().as_str() {
                    "NAN" => Ok(Double(f64::NAN)),
                    "INFINITY" => Ok(Double(f64::INFINITY)),
                    "-INFINITY" => Ok(Double(f64::NEG_INFINITY)),
                    _ => s.parse::<f64>().map(Double).map_err(Error::NotFloat),
                }
            }
            Format::Binary => {
                // PostgreSQL float8 is 8 bytes in network byte order (big-endian)
                if bytes.len() != 8 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut buf = bytes;
                let bits = buf.get_u64();
                Ok(Double(f64::from_bits(bits)))
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => {
                let s = if self.0.is_nan() {
                    "NaN".to_string()
                } else if self.0.is_infinite() {
                    if self.0.is_sign_positive() {
                        "Infinity".to_string()
                    } else {
                        "-Infinity".to_string()
                    }
                } else {
                    self.0.to_string()
                };
                Ok(Bytes::copy_from_slice(s.as_bytes()))
            }
            Format::Binary => {
                let mut buf = BytesMut::new();
                // Write as 8-byte float in network byte order (big-endian)
                buf.put_u64(self.0.to_bits());
                Ok(buf.freeze())
            }
        }
    }
}

impl ToDataRowColumn for Double {
    fn to_data_row_column(&self) -> Data {
        Data::from(self.encode(Format::Text).unwrap_or_else(|_| Bytes::new()))
    }
}

impl From<f64> for Double {
    fn from(value: f64) -> Self {
        Double(value)
    }
}

impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}
