use std::{
    cmp::Ordering,
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use bytes::Buf;
use serde::Deserialize;
use serde::{
    de::{self, Visitor},
    Serialize,
};
use tracing::warn;

use crate::net::messages::data_row::Data;

use super::*;

/// We don't expect NaN's so we're going to implement Ord for this below.
#[derive(PartialEq, Copy, Clone, Debug)]
#[repr(C)]
pub struct Numeric {
    data: f64,
}

impl Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.data.is_nan() {
            warn!("using NaNs in hashing, this breaks aggregates");
        }
        // We don't expect NaNs from Postgres.
        self.data.to_bits().hash(state);
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Deref for Numeric {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Numeric {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Add for Numeric {
    type Output = Numeric;

    fn add(self, rhs: Self) -> Self::Output {
        Numeric {
            data: self.data + rhs.data,
        }
    }
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.data.partial_cmp(&other.data) {
            Some(ordering) => ordering,
            None => {
                if self.data.is_nan() || other.data.is_nan() {
                    warn!("using NaNs in sorting, this doesn't work")
                }
                Ordering::Equal // We don't expect Postgres to send us NaNs.
            }
        }
    }
}

impl Eq for Numeric {}

impl FromDataType for Numeric {
    fn decode(mut bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                Ok(Self { data: s.parse()? })
            }

            Format::Binary => Ok(Self {
                data: match bytes.len() {
                    4 => bytes.get_f32() as f64,
                    8 => bytes.get_f64(),
                    n => return Err(Error::WrongSizeBinary(n)),
                },
            }),
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.data.to_string().as_bytes())),
            Format::Binary => Ok(Bytes::copy_from_slice(self.data.to_be_bytes().as_slice())),
        }
    }
}

impl ToDataRowColumn for Numeric {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

impl From<f32> for Numeric {
    fn from(value: f32) -> Self {
        Self { data: value as f64 }
    }
}

impl From<f64> for Numeric {
    fn from(value: f64) -> Self {
        Self { data: value }
    }
}

struct NumericVisitor;

impl Visitor<'_> for NumericVisitor {
    type Value = Numeric;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a floating point (f32 or f64)")
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric { data: v })
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Numeric { data: v as f64 })
    }
}

impl<'de> Deserialize<'de> for Numeric {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_f64(NumericVisitor)
    }
}

impl Serialize for Numeric {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(self.data)
    }
}
