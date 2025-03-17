use std::{
    cmp::Ordering,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use bytes::Buf;

use crate::net::messages::data_row::Data;

use super::*;

/// We don't expect NaN's so we're going to implement Ord for this below.
#[derive(PartialEq, Copy, Clone, Debug)]
pub struct Numeric {
    data: f64,
}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
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
        match self.partial_cmp(other) {
            Some(ordering) => ordering,
            None => Ordering::Equal, // We don't expect Postgres to send us NaNs.
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
