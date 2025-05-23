use std::str::FromStr;

use uuid::Uuid;

use super::{bigint, uuid, Error};
use crate::{
    config::DataType,
    net::{Format, FromDataType, Vector},
};

#[derive(Debug, Clone)]
pub enum Data<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
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

    pub fn vector(&self) -> Result<Option<Vector>, Error> {
        if self.data_type == DataType::Vector {
            match self.data {
                Data::Text(text) => Ok(Some(Vector::decode(text.as_bytes(), Format::Text)?)),
                Data::Binary(binary) => Ok(Some(Vector::decode(binary, Format::Binary)?)),
            }
        } else {
            Ok(None)
        }
    }

    pub fn hash(&self) -> Result<Option<u64>, Error> {
        match self.data_type {
            DataType::Bigint => match self.data {
                Data::Text(text) => Ok(Some(bigint(text.parse()?))),
                Data::Binary(data) => Ok(Some(bigint(i64::from_be_bytes(data.try_into()?)))),
            },

            DataType::Uuid => match self.data {
                Data::Text(text) => Ok(Some(uuid(Uuid::from_str(text)?))),
                Data::Binary(data) => Ok(Some(uuid(Uuid::from_bytes(data.try_into()?)))),
            },

            DataType::Vector => Ok(None),
        }
    }
}
