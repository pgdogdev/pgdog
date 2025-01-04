//! Query (F) message.
use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// Query (F) message.
#[derive(Debug)]
pub struct Query {
    /// Query string.
    pub query: String,
}

impl Query {
    /// Create new query.
    pub fn new(query: impl ToString) -> Self {
        Self {
            query: query.to_string(),
        }
    }
}

impl FromBytes for Query {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'Q');
        let _len = bytes.get_i32();

        let query = c_string_buf(&mut bytes);

        Ok(Query { query })
    }
}

impl ToBytes for Query {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_string(&self.query);

        Ok(payload.freeze())
    }
}

impl Protocol for Query {
    fn code(&self) -> char {
        'Q'
    }
}
