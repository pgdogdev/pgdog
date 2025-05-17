//! Query (F) message.
use super::prelude::*;

use bytes::Bytes;
use std::str::{from_utf8, from_utf8_unchecked};

/// Query (F) message.
#[derive(Clone)]
pub struct Query {
    /// Query string.
    pub payload: Bytes,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("query", &self.query())
            .finish()
    }
}

impl Query {
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    /// Create new query.
    pub fn new(query: impl ToString) -> Self {
        let mut payload = Payload::named('Q');
        payload.put_string(&query.to_string());
        let payload = payload.freeze();

        Self { payload }
    }

    pub fn query(&self) -> &str {
        // SAFETY:  We check for valid UTF-8 on creation.
        //          Don't read the trailing null byte.
        unsafe { from_utf8_unchecked(&self.payload[5..self.payload.len() - 1]) }
    }
}

impl FromBytes for Query {
    fn from_bytes(payload: Bytes) -> Result<Self, Error> {
        // Check for UTF-8 so we don't have to later.
        from_utf8(&payload[5..payload.len() - 1])?;

        Ok(Query { payload })
    }
}

impl ToBytes for Query {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
    }
}

impl Protocol for Query {
    fn code(&self) -> char {
        'Q'
    }
}

impl<T: ToString> From<T> for Query {
    fn from(value: T) -> Self {
        Query::new(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_query() {
        let query = Query::new("SELECT 1, 2, 3");
        let query = Query::from_bytes(query.to_bytes().unwrap()).unwrap();
        assert_eq!(query.query(), "SELECT 1, 2, 3");
    }
}
