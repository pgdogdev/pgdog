//! Parse (F) message.

use crate::net::c_string_buf;
use std::sync::Arc;

use super::code;
use super::prelude::*;

/// Parse (F) message.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Default)]
pub struct Parse {
    /// Prepared statement name.
    name: Arc<String>,
    /// Prepared statement query.
    query: Arc<String>,
    /// List of data types if any are declared.
    data_types: Arc<Vec<i32>>,
    /// Original payload.
    original: Option<Bytes>,
}

impl Parse {
    pub fn len(&self) -> usize {
        self.name.len() + 1
        + self.query.len() + 1
        + 2 // number of params
        + self.data_types().len() * 4
        + 4 // len
        + 1 // code
    }

    /// New anonymous prepared statement.
    #[cfg(test)]
    pub fn new_anonymous(query: &str) -> Self {
        Self {
            name: Arc::new("".into()),
            query: Arc::new(query.to_string()),
            data_types: Arc::new(vec![]),
            original: None,
        }
    }

    /// New prepared statement.
    pub fn named(name: impl ToString, query: impl ToString) -> Self {
        Self {
            name: Arc::new(name.to_string()),
            query: Arc::new(query.to_string()),
            data_types: Arc::new(vec![]),
            original: None,
        }
    }

    /// Anonymous prepared statement.
    pub fn anonymous(&self) -> bool {
        self.name.is_empty()
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    /// Get query reference.
    pub fn query_ref(&self) -> Arc<String> {
        self.query.clone()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rename(&self, name: &str) -> Parse {
        let mut parse = self.clone();
        parse.name = Arc::new(name.to_owned());
        parse
    }

    pub fn data_types(&self) -> &[i32] {
        &self.data_types
    }

    pub fn data_types_ref(&self) -> Arc<Vec<i32>> {
        self.data_types.clone()
    }
}

impl FromBytes for Parse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'P');
        let _len = bytes.get_i32();
        let name = c_string_buf(&mut bytes);
        let query = c_string_buf(&mut bytes);
        let params = bytes.get_i16() as usize;
        let data_types = (0..params).map(|_| bytes.get_i32()).collect::<Vec<_>>();

        Ok(Self {
            name: Arc::new(name),
            query: Arc::new(query),
            data_types: Arc::new(data_types),
            original: Some(original),
        })
    }
}

impl ToBytes for Parse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        // Fast path when the contents haven't been changed.
        if let Some(ref original) = self.original {
            return Ok(original.clone());
        }

        let mut payload = Payload::named(self.code());
        payload.reserve(self.len());

        payload.put_string(&self.name);
        payload.put_string(&self.query);
        payload.put_i16(self.data_types.len() as i16);

        for type_ in self.data_types() {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse() {
        let parse = Parse::named("test", "SELECT $1");
        let b = parse.to_bytes().unwrap();
        assert_eq!(parse.len(), b.len());
    }
}
