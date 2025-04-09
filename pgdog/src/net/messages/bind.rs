//! Bind (F) message.
use crate::net::c_string_buf;
use uuid::Uuid;

use super::code;
use super::prelude::*;
use super::Error;
use super::FromDataType;
use super::Vector;

use std::str::from_utf8;

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum Format {
    Text,
    Binary,
}

impl From<Format> for i16 {
    fn from(val: Format) -> Self {
        match val {
            Format::Text => 0,
            Format::Binary => 1,
        }
    }
}

/// Parameter data.
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Parameter {
    /// Parameter data length.
    pub len: i32,
    /// Parameter data.
    pub data: Vec<u8>,
}

impl Parameter {
    pub fn len(&self) -> usize {
        4 + self.data.len()
    }
}

/// Parameter with encoded format.
#[derive(Debug, Clone)]
pub struct ParameterWithFormat<'a> {
    parameter: &'a Parameter,
    format: Format,
}

impl ParameterWithFormat<'_> {
    /// Get text representation if it's valid UTF-8.
    pub fn text(&self) -> Option<&str> {
        from_utf8(&self.parameter.data).ok()
    }

    /// Get BIGINT if one is encoded in the field.
    pub fn bigint(&self) -> Option<i64> {
        Self::decode(self)
    }

    /// Get UUID, if one is encoded in the field.
    pub fn uuid(&self) -> Option<Uuid> {
        Self::decode(self)
    }

    /// Get vector, if one is encoded in the field.
    pub fn vector(&self) -> Option<Vector> {
        Self::decode(self)
    }

    /// Get decoded value.
    pub fn decode<T: FromDataType>(&self) -> Option<T> {
        T::decode(&self.parameter.data, self.format).ok()
    }

    pub fn format(&self) -> Format {
        self.format
    }

    pub fn data(&self) -> &[u8] {
        &self.parameter.data
    }
}

/// Bind (F) message.
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Ord, Eq)]
pub struct Bind {
    /// Portal name.
    pub portal: String,
    /// Prepared statement name.
    pub statement: String,
    /// Format codes.
    pub codes: Vec<i16>,
    /// Parameters.
    pub params: Vec<Parameter>,
    /// Results format.
    pub results: Vec<i16>,
}

impl Bind {
    pub(crate) fn len(&self) -> usize {
        self.portal.len()
            + 1 // NULL
            + self.statement.len()
            + 1 // NULL
            + self.codes.len() * std::mem::size_of::<i16>() + 2 // num codes
            + self.params.iter().map(|p| p.len()).sum::<usize>() + 2 // num params
            + self.results.len() * std::mem::size_of::<i16>() + 2 // num results
            + 4 // len
            + 1 // code
    }

    /// Format a parameter is using.
    pub(crate) fn parameter_format(&self, index: usize) -> Result<Format, Error> {
        let code = if self.codes.len() == self.params.len() {
            self.codes.get(index).copied()
        } else if self.codes.len() == 1 {
            self.codes.first().copied()
        } else {
            Some(0)
        };

        if let Some(code) = code {
            match code {
                0 => Ok(Format::Text),
                1 => Ok(Format::Binary),
                _ => Err(Error::IncorrectParameterFormatCode(code)),
            }
        } else {
            Ok(Format::Text)
        }
    }

    /// Get parameter at index.
    pub(crate) fn parameter(&self, index: usize) -> Result<Option<ParameterWithFormat<'_>>, Error> {
        let format = self.parameter_format(index)?;
        Ok(self
            .params
            .get(index)
            .map(|parameter| ParameterWithFormat { parameter, format }))
    }

    /// Rename this Bind message to a different prepared statement.
    pub fn rename(mut self, name: impl ToString) -> Self {
        self.statement = name.to_string();
        self
    }

    /// Is this Bind message anonymous?
    pub fn anonymous(&self) -> bool {
        self.statement.is_empty()
    }

    /// Format codes, if any.
    pub fn codes(&self) -> Vec<Format> {
        self.codes
            .iter()
            .map(|c| {
                if *c == 0 {
                    Format::Text
                } else {
                    Format::Binary
                }
            })
            .collect()
    }
}

impl FromBytes for Bind {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'B');
        let _len = bytes.get_i32();
        let portal = c_string_buf(&mut bytes);
        let statement = c_string_buf(&mut bytes);
        let num_codes = bytes.get_i16();
        let codes = (0..num_codes).map(|_| bytes.get_i16()).collect();
        let num_params = bytes.get_i16();
        let params = (0..num_params)
            .map(|_| {
                let len = bytes.get_i32();
                let data = if len >= 0 {
                    let mut data = Vec::with_capacity(len as usize);
                    (0..len).for_each(|_| data.push(bytes.get_u8()));
                    data
                } else {
                    vec![]
                };
                Parameter { len, data }
            })
            .collect();
        let num_results = bytes.get_i16();
        let results = (0..num_results).map(|_| bytes.get_i16()).collect();

        Ok(Self {
            portal,
            statement,
            codes,
            params,
            results,
        })
    }
}

impl ToBytes for Bind {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_string(&self.portal);
        payload.put_string(&self.statement);
        payload.put_i16(self.codes.len() as i16);
        for code in &self.codes {
            payload.put_i16(*code);
        }
        payload.put_i16(self.params.len() as i16);
        for param in &self.params {
            payload.put_i32(param.len);
            payload.put_slice(param.data.as_slice());
        }
        payload.put_i16(self.results.len() as i16);
        for result in &self.results {
            payload.put_i16(*result);
        }
        Ok(payload.freeze())
    }
}

impl Protocol for Bind {
    fn code(&self) -> char {
        'B'
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        backend::pool::{test::pool, Request},
        net::messages::ErrorResponse,
    };

    #[tokio::test]
    async fn test_bind() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let bind = Bind {
            portal: "".into(),
            statement: "__pgdog_1".into(),
            codes: vec![1, 0],
            params: vec![
                Parameter {
                    len: 2,
                    data: vec![0, 1],
                },
                Parameter {
                    len: 4,
                    data: "test".as_bytes().to_vec(),
                },
            ],
            results: vec![0],
        };

        let bytes = bind.to_bytes().unwrap();
        assert_eq!(Bind::from_bytes(bytes.clone()).unwrap(), bind);
        assert_eq!(bind.len(), bytes.len());
        let mut c = bytes.clone();
        let _ = c.get_u8();
        let len = c.get_i32();

        assert_eq!(len as usize + 1, bytes.len());

        conn.send(vec![bind.message().unwrap()]).await.unwrap();
        let res = conn.read().await.unwrap();
        let err = ErrorResponse::from_bytes(res.to_bytes().unwrap()).unwrap();
        assert_eq!(err.code, "26000");
    }
}
