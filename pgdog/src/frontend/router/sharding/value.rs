use std::str::{from_utf8, FromStr};

use uuid::Uuid;

use super::{Error, Hasher};
use crate::{
    config::DataType,
    net::{Format, FromDataType, ParameterWithFormat, Vector},
};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum Data<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
    Integer(i64),
}

impl<'a> From<&'a str> for Data<'a> {
    fn from(value: &'a str) -> Self {
        Self::Text(value)
    }
}

impl<'a> From<&'a [u8]> for Data<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Binary(value)
    }
}

impl From<i64> for Data<'_> {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl<'a> From<&'a Bytes> for Data<'a> {
    fn from(value: &'a Bytes) -> Self {
        Self::Binary(&value[..])
    }
}

#[derive(Debug, Clone)]
pub struct Value<'a> {
    data_type: DataType,
    data: Data<'a>,
}

impl<'a> Value<'a> {
    pub fn new(data: impl Into<Data<'a>>, data_type: DataType) -> Self {
        Self {
            data_type,
            data: data.into(),
        }
    }

    pub fn from_param(
        param: &'a ParameterWithFormat<'a>,
        data_type: DataType,
    ) -> Result<Self, Error> {
        let data = param.data();
        let format = param.format();

        match format {
            Format::Text => Ok(Self::new(from_utf8(data)?, data_type)),
            Format::Binary => Ok(Self::new(data, data_type)),
        }
    }

    pub fn vector(&self) -> Result<Option<Vector>, Error> {
        if self.data_type == DataType::Vector {
            match self.data {
                Data::Text(text) => Ok(Some(Vector::decode(text.as_bytes(), Format::Text)?)),
                Data::Binary(binary) => Ok(Some(Vector::decode(binary, Format::Binary)?)),
                Data::Integer(_) => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn valid(&self) -> bool {
        match self.data_type {
            DataType::Bigint => match self.data {
                Data::Text(text) => text.parse::<i64>().is_ok(),
                Data::Binary(data) => [2, 4, 8].contains(&data.len()),
                Data::Integer(_) => true,
            },
            DataType::Uuid => match self.data {
                Data::Text(text) => Uuid::from_str(text).is_ok(),
                Data::Binary(data) => data.len() == 16,
                Data::Integer(_) => false,
            },
            DataType::Varchar => match self.data {
                Data::Text(_) => true,
                Data::Binary(data) => from_utf8(data).is_ok(),
                Data::Integer(_) => false,
            },

            _ => false,
        }
    }

    pub fn data(&self) -> &Data<'_> {
        &self.data
    }

    pub fn integer(&self) -> Result<Option<i64>, Error> {
        if self.data_type == DataType::Bigint {
            match self.data {
                Data::Integer(int) => Ok(Some(int)),
                Data::Text(text) => Ok(Some(text.parse()?)),
                Data::Binary(data) => match data.len() {
                    2 => Ok(Some(i16::from_be_bytes(data.try_into()?) as i64)),
                    4 => Ok(Some(i32::from_be_bytes(data.try_into()?) as i64)),
                    8 => Ok(Some(i64::from_be_bytes(data.try_into()?))),
                    _ => Err(Error::IntegerSize),
                },
            }
        } else {
            Ok(None)
        }
    }

    pub fn varchar(&self) -> Result<Option<&str>, Error> {
        if self.data_type == DataType::Varchar {
            match self.data {
                Data::Integer(_) => Ok(None),
                Data::Text(text) => Ok(Some(text)),
                Data::Binary(data) => Ok(Some(from_utf8(data)?)),
            }
        } else {
            Ok(None)
        }
    }

    pub fn uuid(&self) -> Result<Option<Uuid>, Error> {
        if self.data_type != DataType::Uuid {
            return Ok(None);
        }

        let uuid = match &self.data {
            Data::Text(text) => Uuid::parse_str(text)?,
            Data::Binary(data) => Uuid::from_slice(data)?,
            _ => return Ok(None),
        };

        Ok(Some(uuid))
    }

    pub fn hash(&self, hasher: Hasher) -> Result<Option<u64>, Error> {
        match self.data_type {
            DataType::Bigint => match self.data {
                Data::Text(text) => Ok(Some(hasher.bigint(text.parse()?))),
                Data::Binary(data) => Ok(Some(hasher.bigint(match data.len() {
                    2 => i16::from_be_bytes(data.try_into()?) as i64,
                    4 => i32::from_be_bytes(data.try_into()?) as i64,
                    8 => i64::from_be_bytes(data.try_into()?),
                    _ => return Err(Error::IntegerSize),
                }))),
                Data::Integer(int) => Ok(Some(hasher.bigint(int))),
            },

            DataType::Uuid => match self.data {
                Data::Text(text) => Ok(Some(hasher.uuid(Uuid::from_str(text)?))),
                Data::Binary(data) => Ok(Some(hasher.uuid(Uuid::from_bytes(data.try_into()?)))),
                Data::Integer(_) => Ok(None),
            },

            DataType::Vector => Ok(None),
            DataType::Varchar => match self.data {
                Data::Binary(b) => Ok(Some(hasher.varchar(b))),
                Data::Text(s) => Ok(Some(hasher.varchar(s.as_bytes()))),
                Data::Integer(_) => Ok(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn uuid_binary_parses_correctly() -> Result<(), Error> {
        let raw_bytes: [u8; 16] = [0x11u8; 16];
        let expected_uuid = Uuid::parse_str("11111111-1111-1111-1111-111111111111")?;

        let value = Value {
            data_type: DataType::Uuid,
            data: Data::Binary(&raw_bytes),
        };

        assert_eq!(value.uuid()?, Some(expected_uuid));
        Ok(())
    }
}
