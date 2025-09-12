use crate::{
    frontend::router::sharding::vector::Distance,
    net::{
        messages::{Format, ToDataRowColumn},
        Error,
    },
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{
    de::{self, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};
use std::{fmt::Debug, ops::Deref, str::from_utf8};

use super::{Datum, Float, FromDataType};

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
#[repr(C)]
pub struct Vector {
    values: Vec<Float>,
}

impl Debug for Vector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.values.len() > 3 {
            f.debug_struct("Vector")
                .field(
                    "values",
                    &format!(
                        "[{}..{}]",
                        self.values[0],
                        self.values[self.values.len() - 1]
                    ),
                )
                .finish()
        } else {
            f.debug_struct("Vector")
                .field("values", &self.values)
                .finish()
        }
    }
}

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

impl Vector {
    /// Length of the vector.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Is the vector empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compute L2 distance between the vectors.
    pub fn distance_l2(&self, other: &Self) -> f32 {
        Distance::Euclidean(self, other).distance()
    }
}

impl Deref for Vector {
    type Target = Vec<Float>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl From<&[f64]> for Vector {
    fn from(value: &[f64]) -> Self {
        Self {
            values: value.iter().map(|v| Float(*v as f32)).collect(),
        }
    }
}

impl From<&[f32]> for Vector {
    fn from(value: &[f32]) -> Self {
        Self {
            values: value.iter().map(|v| Float(*v)).collect(),
        }
    }
}

impl From<Vec<f32>> for Vector {
    fn from(value: Vec<f32>) -> Self {
        Self {
            values: value.into_iter().map(Float::from).collect(),
        }
    }
}

impl From<Vec<f64>> for Vector {
    fn from(value: Vec<f64>) -> Self {
        Self {
            values: value.into_iter().map(|v| Float(v as f32)).collect(),
        }
    }
}

impl From<Vec<Float>> for Vector {
    fn from(value: Vec<Float>) -> Self {
        Self { values: value }
    }
}

impl TryFrom<&str> for Vector {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::decode(value.as_bytes(), Format::Text)
    }
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

struct VectorVisitor;

impl<'de> Visitor<'de> for VectorVisitor {
    type Value = Vector;

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut results = vec![];
        while let Some(n) = seq.next_element::<f64>()? {
            results.push(n);
        }

        Ok(Vector::from(results.as_slice()))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected a list of floating points")
    }
}

impl<'de> Deserialize<'de> for Vector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(VectorVisitor)
    }
}

impl Serialize for Vector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for v in &self.values {
            seq.serialize_element(v)?;
        }
        seq.end()
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

    #[test]
    fn test_vector_distance_with_special_values() {
        // Test distance calculation with normal values
        let v1 = Vector::from(&[3.0, 4.0][..]);
        let v2 = Vector::from(&[0.0, 0.0][..]);
        let distance = v1.distance_l2(&v2);
        assert_eq!(distance, 5.0); // 3-4-5 triangle

        // Test distance with NaN
        let v_nan = Vector::from(vec![Float(f32::NAN), Float(1.0)]);
        let v_normal = Vector::from(&[1.0, 1.0][..]);
        let distance_nan = v_nan.distance_l2(&v_normal);
        assert!(distance_nan.is_nan());

        // Test distance with Infinity
        let v_inf = Vector::from(vec![Float(f32::INFINITY), Float(1.0)]);
        let distance_inf = v_inf.distance_l2(&v_normal);
        assert!(distance_inf.is_infinite());
    }
}
