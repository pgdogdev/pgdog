//! Bind (F) message.
use crate::net::c_string_buf;
use pgdog_plugin::bindings::Parameter as PluginParameter;
use uuid::Uuid;

use super::code;
use super::prelude::*;
use super::Error;
use super::FromDataType;

use std::cmp::max;
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
#[derive(Debug, Clone)]
pub struct Parameter {
    /// Parameter data length.
    pub len: i32,
    /// Parameter data.
    pub data: Vec<u8>,
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
        Self::decode(&self)
    }

    /// Get UUID, if one is encoded in the field.
    pub fn uuid(&self) -> Option<Uuid> {
        Self::decode(&self)
    }

    /// Get decoded value.
    pub fn decode<T: FromDataType>(&self) -> Option<T> {
        T::decode(&self.parameter.data, self.format).ok()
    }
}

/// Bind (F) message.
#[derive(Debug, Clone, Default)]
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
    /// Format a parameter is using.
    pub fn parameter_format(&self, index: usize) -> Result<Format, Error> {
        let index = max(self.codes.len() as isize - 1, index as isize) as usize;
        if let Some(code) = self.codes.get(index) {
            match code {
                0 => Ok(Format::Text),
                1 => Ok(Format::Binary),
                _ => Err(Error::IncorrectParameterFormatCode(*code)),
            }
        } else {
            Ok(Format::Text)
        }
    }

    /// Get parameter at index.
    pub fn parameter(&self, index: usize) -> Result<Option<ParameterWithFormat<'_>>, Error> {
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

    /// Convert bind parameters to plugin parameters.
    ///
    /// # Safety
    ///
    /// This function allocates memory the caller has to deallocate.
    pub unsafe fn plugin_parameters(&self) -> Result<Vec<PluginParameter>, Error> {
        let mut params = vec![];

        for (index, param) in self.params.iter().enumerate() {
            let format = self.parameter_format(index)?;
            params.push(PluginParameter::new(format.into(), &param.data));
        }

        Ok(params)
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
                let mut data = vec![];
                (0..len).for_each(|_| data.push(bytes.get_u8()));
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
            for b in &param.data {
                payload.put_u8(*b);
            }
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
