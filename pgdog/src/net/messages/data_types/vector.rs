use std::str::from_utf8;

use crate::net::{
    messages::{Format, ToDataRowColumn},
    Error,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Datum, FromDataType};
use pgdog_vector::Float;

pub use pgdog_vector::Vector;

impl FromDataType for Vector {
    fn decode(mut bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Binary => {
                let mut values = vec![];
                while bytes.len() >= std::mem::size_of::<f32>() {
                    values.push(Float(bytes.get_f32()));
                }
                Ok(Self { values })
            }
            Format::Text => {
                let no_brackets = &bytes[1..bytes.len() - 1];
                let floats = no_brackets
                    .split(|n| n == &b',')
                    .flat_map(|b| from_utf8(b).map(|n| n.trim().parse::<f32>().ok()))
                    .flatten()
                    .map(Float::from)
                    .collect();
                Ok(Self { values: floats })
            }
        }
    }

    fn encode(&self, encoding: Format) -> Result<bytes::Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::from(format!(
                "[{}]",
                self.values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ))),
            Format::Binary => {
                let mut bytes = BytesMut::new();
                for float in &self.values {
                    bytes.put_f32(float.0);
                }
                Ok(bytes.freeze())
            }
        }
    }
}

impl ToDataRowColumn for Vector {
    fn to_data_row_column(&self) -> crate::net::messages::data_row::Data {
        self.encode(Format::Text).unwrap().into()
    }
}

pub fn str_to_vector(value: &str) -> Result<Vector, Error> {
    FromDataType::decode(value.as_bytes(), Format::Text)
}

impl From<Vector> for Datum {
    fn from(val: Vector) -> Self {
        Datum::Vector(val)
    }
}

impl TryFrom<Datum> for Vector {
    type Error = Error;

    fn try_from(value: Datum) -> Result<Self, Self::Error> {
        match value {
            Datum::Vector(vector) => Ok(vector),
            Datum::Unknown(data) => Vector::decode(&data, Format::Text), // Try decoding anyway.
            _ => Err(Error::UnexpectedPayload),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_vectors() {
        let v = "[1,2,3]";
        let vector = Vector::decode(v.as_bytes(), Format::Text).unwrap();
        assert_eq!(vector.values[0], Float(1.0));
        assert_eq!(vector.values[1], Float(2.0));
        assert_eq!(vector.values[2], Float(3.0));
        let b = vector.encode(Format::Text).unwrap();
        assert_eq!(&b, &"[1,2,3]");

        let mut v = vec![];
        v.extend(1.0_f32.to_be_bytes());
        v.extend(2.0_f32.to_be_bytes());
        v.extend(3.0_f32.to_be_bytes());
        let vector = Vector::decode(v.as_slice(), Format::Binary).unwrap();
        assert_eq!(vector.values[0], Float(1.0));
        assert_eq!(vector.values[1], Float(2.0));
        assert_eq!(vector.values[2], Float(3.0));
    }

    #[test]
    fn test_vector_with_nan_and_infinity() {
        // Test text format with NaN and Infinity
        let v = "[1.5,NaN,Infinity,-Infinity,2.5]";
        let vector = Vector::decode(v.as_bytes(), Format::Text).unwrap();
        assert_eq!(vector.values[0], Float(1.5));
        assert!(vector.values[1].0.is_nan());
        assert!(vector.values[2].0.is_infinite() && vector.values[2].0.is_sign_positive());
        assert!(vector.values[3].0.is_infinite() && vector.values[3].0.is_sign_negative());
        assert_eq!(vector.values[4], Float(2.5));

        // Test binary format with special values
        let mut v = vec![];
        v.extend(1.5_f32.to_be_bytes());
        v.extend(f32::NAN.to_be_bytes());
        v.extend(f32::INFINITY.to_be_bytes());
        v.extend(f32::NEG_INFINITY.to_be_bytes());
        v.extend(2.5_f32.to_be_bytes());

        let vector = Vector::decode(v.as_slice(), Format::Binary).unwrap();
        assert_eq!(vector.values[0], Float(1.5));
        assert!(vector.values[1].0.is_nan());
        assert_eq!(vector.values[2], Float(f32::INFINITY));
        assert_eq!(vector.values[3], Float(f32::NEG_INFINITY));
        assert_eq!(vector.values[4], Float(2.5));

        // Test encoding back to text
        let encoded = vector.encode(Format::Text).unwrap();
        let encoded_str = String::from_utf8_lossy(&encoded);
        assert!(encoded_str.contains("NaN"));
        assert!(encoded_str.contains("Infinity"));
        assert!(encoded_str.contains("-Infinity"));
    }
}
