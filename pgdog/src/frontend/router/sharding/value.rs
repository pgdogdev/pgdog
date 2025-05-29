use std::str::{from_utf8, FromStr};

use uuid::Uuid;

use super::{bigint, uuid, varchar, Error};
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

impl<'a> From<i64> for Data<'a> {
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

            _ => false,
        }
    }

    pub fn hash(&self) -> Result<Option<u64>, Error> {
        match self.data_type {
            DataType::Bigint => match self.data {
                Data::Text(text) => Ok(Some(bigint(text.parse()?))),
                Data::Binary(data) => Ok(Some(bigint(match data.len() {
                    2 => i16::from_be_bytes(data.try_into()?) as i64,
                    4 => i32::from_be_bytes(data.try_into()?) as i64,
                    8 => i64::from_be_bytes(data.try_into()?) as i64,
                    _ => return Err(Error::IntegerSize),
                }))),
                Data::Integer(int) => Ok(Some(bigint(int))),
            },

            DataType::Uuid => match self.data {
                Data::Text(text) => Ok(Some(uuid(Uuid::from_str(text)?))),
                Data::Binary(data) => Ok(Some(uuid(Uuid::from_bytes(data.try_into()?)))),
                Data::Integer(_) => Ok(None),
            },

            DataType::Vector => Ok(None),
            DataType::Varchar => match self.data {
                Data::Binary(b) => Ok(varchar(b).ok()),
                Data::Text(s) => Ok(Some(varchar(s.as_bytes())?)),
                Data::Integer(_) => Ok(None),
            },
        }
    }
}
