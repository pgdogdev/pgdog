use bytes::{Buf, BufMut, BytesMut};
use std::cmp::Ordering;

use crate::net::messages::data_row::Data;

use super::*;

/// Wrapper type for f32 that implements Ord for PostgreSQL compatibility
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Float(pub f32);

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // PostgreSQL ordering: NaN is greater than all other values
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => self.0.partial_cmp(&other.0),
        }
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Eq for Float {}

impl FromDataType for Float {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                match s.to_uppercase().as_str() {
                    "NAN" => Ok(Float(f32::NAN)),
                    "INFINITY" => Ok(Float(f32::INFINITY)),
                    "-INFINITY" => Ok(Float(f32::NEG_INFINITY)),
                    _ => s.parse::<f32>().map(Float).map_err(Error::NotFloat),
                }
            }
            Format::Binary => {
                // PostgreSQL float4 is 4 bytes in network byte order (big-endian)
                if bytes.len() != 4 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }

                let mut buf = bytes;
                let bits = buf.get_u32();
                Ok(Float(f32::from_bits(bits)))
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
                // Write as 4-byte float in network byte order (big-endian)
                buf.put_u32(self.0.to_bits());
                Ok(buf.freeze())
            }
        }
    }
}

impl ToDataRowColumn for Float {
    fn to_data_row_column(&self) -> Data {
        Data::from(self.encode(Format::Text).unwrap_or_else(|_| Bytes::new()))
    }
}

impl From<f32> for Float {
    fn from(value: f32) -> Self {
        Float(value)
    }
}

impl From<Float> for f32 {
    fn from(value: Float) -> Self {
        value.0
    }
}
