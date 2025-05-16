//! Bind (F) message.
use crate::net::c_string_buf_len;
use uuid::Uuid;

use super::code;
use super::prelude::*;
use super::Error;
use super::FromDataType;
use super::Vector;

use std::fmt::Debug;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

#[derive(PartialEq, Debug, Copy, Clone, PartialOrd, Ord, Eq)]
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
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Parameter {
    /// Parameter data length.
    pub len: i32,
    /// Parameter data.
    pub data: Vec<u8>,
}

impl Debug for Parameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Parameter");
        if let Ok(text) = from_utf8(&self.data) {
            debug.field("data", &text);
        } else {
            debug.field("data", &self.data);
        }
        debug.field("len", &self.len);
        debug.finish()
    }
}

impl Parameter {
    pub(crate) fn len(&self) -> usize {
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
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Bind {
    /// Portal name.
    portal: Bytes,
    /// Prepared statement name.
    statement: Bytes,
    /// Format codes.
    codes: Vec<Format>,
    /// Parameters.
    params: Vec<Parameter>,
    /// Results format.
    results: Vec<i16>,
    /// Original payload.
    original: Option<Bytes>,
}

impl Default for Bind {
    fn default() -> Self {
        Bind {
            portal: Bytes::from("\0"),
            statement: Bytes::from("\0"),
            codes: vec![],
            params: vec![],
            results: vec![],
            original: None,
        }
    }
}

impl Bind {
    pub(crate) fn len(&self) -> usize {
        self.portal.len()
            + self.statement.len()
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
            Some(Format::Text)
        };

        Ok(code.unwrap_or(Format::Text))
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
        self.statement = Bytes::from(name.to_string() + "\0");
        self.original = None;
        self
    }

    /// Is this Bind message anonymous?
    pub fn anonymous(&self) -> bool {
        self.statement.len() == 1
    }

    #[inline]
    pub(crate) fn statement(&self) -> &str {
        // SAFETY: We check that this is valid UTF-8 in FromBytes::from_bytes below.
        unsafe { from_utf8_unchecked(&self.statement[0..self.statement.len() - 1]) }
    }

    /// Format codes, if any.
    pub fn codes(&self) -> &[Format] {
        &self.codes
    }
}

#[cfg(test)]
impl Bind {
    pub(crate) fn test_statement(name: &str) -> Self {
        Self {
            statement: Bytes::from(name.to_string() + "\0"),
            ..Default::default()
        }
    }

    pub(crate) fn test_params(name: &str, params: &[Parameter]) -> Self {
        Self {
            statement: Bytes::from(name.to_string() + "\0"),
            params: params.to_vec(),
            ..Default::default()
        }
    }

    pub(crate) fn test_name_portal(name: &str, portal: &str) -> Self {
        Self {
            statement: Bytes::from(name.to_string() + "\0"),
            portal: Bytes::from(portal.to_string() + "\0"),
            ..Default::default()
        }
    }

    pub(crate) fn test_params_codes(name: &str, params: &[Parameter], codes: &[Format]) -> Self {
        Self {
            statement: Bytes::from(name.to_string() + "\0"),
            codes: codes.to_vec(),
            params: params.to_vec(),
            ..Default::default()
        }
    }
}

impl FromBytes for Bind {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'B');
        let _len = bytes.get_i32();

        let portal = bytes.split_to(c_string_buf_len(&bytes));
        let statement = bytes.split_to(c_string_buf_len(&bytes));
        from_utf8(&portal[0..portal.len() - 1])?;
        from_utf8(&statement[0..statement.len() - 1])?;

        let num_codes = bytes.get_i16();
        let codes = (0..num_codes)
            .map(|_| match bytes.get_i16() {
                0 => Format::Text,
                _ => Format::Binary,
            })
            .collect();
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
            original: Some(original),
        })
    }
}

impl ToBytes for Bind {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        // Fast path.
        if let Some(ref original) = self.original {
            return Ok(original.clone());
        }

        let mut payload = Payload::named(self.code());
        payload.reserve(self.len());

        payload.put(self.portal.clone());
        payload.put(self.statement.clone());
        payload.put_i16(self.codes.len() as i16);
        for code in &self.codes {
            payload.put_i16(match code {
                Format::Text => 0,
                Format::Binary => 1,
            });
        }
        payload.put_i16(self.params.len() as i16);
        for param in &self.params {
            payload.put_i32(param.len);
            payload.put(&param.data[..]);
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
        backend::{
            pool::{test::pool, Request},
            server::test::test_server,
            ProtocolMessage,
        },
        net::{messages::ErrorResponse, DataRow, Execute, Parse, Sync},
    };

    #[tokio::test]
    async fn test_bind() {
        let pool = pool();
        let mut conn = pool.get(&Request::default()).await.unwrap();
        let bind = Bind {
            original: None,
            portal: "\0".into(),
            statement: "__pgdog_1\0".into(),
            codes: vec![Format::Binary, Format::Text],
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
        let mut original = Bind::from_bytes(bytes.clone()).unwrap();
        original.original = None;
        assert_eq!(original, bind);
        assert_eq!(bind.len(), bytes.len());
        let mut c = bytes.clone();
        let _ = c.get_u8();
        let len = c.get_i32();

        assert_eq!(len as usize + 1, bytes.len());

        conn.send(&vec![ProtocolMessage::from(bind)].into())
            .await
            .unwrap();
        let res = conn.read().await.unwrap();
        let err = ErrorResponse::from_bytes(res.to_bytes().unwrap()).unwrap();
        assert_eq!(err.code, "26000");

        let anon = Bind::default();
        assert!(anon.anonymous());
    }

    #[tokio::test]
    async fn test_jsonb() {
        let mut server = test_server().await;
        let parse = Parse::named("test", "SELECT $1::jsonb");
        let binary_marker = String::from("\u{1}");
        let json = r#"[{"name": "force_database_error", "type": "C", "value": "false"}, {"name": "__dbver__", "type": "C", "value": 2}]"#;
        let jsonb = binary_marker + json;
        let bind = Bind {
            statement: "test\0".into(),
            codes: vec![Format::Binary],
            params: vec![Parameter {
                data: jsonb.as_bytes().to_vec(),
                len: jsonb.len() as i32,
            }],
            ..Default::default()
        };
        let execute = Execute::new();
        server
            .send(
                &vec![
                    ProtocolMessage::from(parse),
                    bind.into(),
                    execute.into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', '2', 'D', 'C', 'Z'] {
            let msg = server.read().await.unwrap();
            if msg.code() == 'E' {
                let err = ErrorResponse::from_bytes(msg.to_bytes().unwrap()).unwrap();
                panic!("{:?}", err);
            }

            if msg.code() == 'D' {
                let dr = DataRow::from_bytes(msg.to_bytes().unwrap()).unwrap();
                let r = dr.get::<String>(0, Format::Binary).unwrap();
                assert_eq!(r, json);
            }
            assert_eq!(msg.code(), c);
        }
    }
}
