//! Parse (F) message.

use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// Parse (F) message.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Parse {
    /// Prepared statement name.
    pub name: String,
    /// Prepared statement query.
    pub query: String,
    /// List of data types if any are declared.
    pub data_types: Vec<i32>,
}

impl Parse {
    /// New anonymous prepared statement.
    pub fn new_anonymous(query: &str) -> Self {
        Self {
            name: "".into(),
            query: query.to_string(),
            data_types: vec![],
        }
    }

    /// New prepared statement.
    pub fn numbered(name: impl ToString, query: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            query: query.to_string(),
            data_types: vec![],
        }
    }

    /// Anonymous prepared statement.
    pub fn anonymous(&self) -> bool {
        self.name.is_empty()
    }
}

impl FromBytes for Parse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'P');
        let _len = bytes.get_i32();
        let name = c_string_buf(&mut bytes);
        let query = c_string_buf(&mut bytes);
        let params = bytes.get_i16() as usize;
        let data_types = (0..params).map(|_| bytes.get_i32()).collect::<Vec<_>>();

        Ok(Self {
            name,
            query,
            data_types,
        })
    }
}

impl ToBytes for Parse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());

        payload.put_string(&self.name);
        payload.put_string(&self.query);
        payload.put_i16(self.data_types.len() as i16);

        for type_ in &self.data_types {
            payload.put_i32(*type_);
        }

        Ok(payload.freeze())
    }
}

impl Protocol for Parse {
    fn code(&self) -> char {
        'P'
    }
}
