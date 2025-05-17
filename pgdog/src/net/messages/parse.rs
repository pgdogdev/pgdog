//! Parse (F) message.

use crate::net::c_string_buf_len;
use std::fmt::Debug;
use std::io::Cursor;
use std::mem::size_of;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use super::code;
use super::prelude::*;

/// Parse (F) message.
#[derive(Clone, Hash, Eq, PartialEq, Default)]
pub struct Parse {
    /// Prepared statement name.
    name: Bytes,
    /// Prepared statement query.
    query: Bytes,
    /// List of data types if any are declared.
    data_types: Bytes,
    /// Original payload.
    original: Option<Bytes>,
}

impl Debug for Parse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parse")
            .field("name", &self.name())
            .field("query", &self.query())
            .field("modified", &self.original.is_none())
            .finish()
    }
}

impl Parse {
    pub fn len(&self) -> usize {
        self.name.len() + self.query.len() + self.data_types.len() + 5
    }

    /// New anonymous prepared statement.
    #[cfg(test)]
    pub fn new_anonymous(query: &str) -> Self {
        Self {
            name: Bytes::from("\0"),
            query: Bytes::from(query.to_owned() + "\0"),
            data_types: Bytes::copy_from_slice(&0i16.to_be_bytes()),
            original: None,
        }
    }

    /// New prepared statement.
    pub fn named(name: impl ToString, query: impl ToString) -> Self {
        Self {
            name: Bytes::from(name.to_string() + "\0"),
            query: Bytes::from(query.to_string() + "\0"),
            data_types: Bytes::copy_from_slice(&0i16.to_be_bytes()),
            original: None,
        }
    }

    /// Anonymous prepared statement.
    pub fn anonymous(&self) -> bool {
        self.name.len() == 1 // Just the null byte.
    }

    pub fn query(&self) -> &str {
        // SAFETY: We check that this is valid UTF-8 in Self::from_bytes.
        unsafe { from_utf8_unchecked(&self.query[0..self.query.len() - 1]) }
    }

    /// Get query reference.
    pub fn query_ref(&self) -> Bytes {
        self.query.clone()
    }

    pub fn name(&self) -> &str {
        // SAFETY: We check that this is valid UTF-8 in Self::from_bytes.
        unsafe { from_utf8_unchecked(&self.name[0..self.name.len() - 1]) }
    }

    pub fn rename(&self, name: &str) -> Parse {
        let mut parse = self.clone();
        parse.name = Bytes::from(name.to_string() + "\0");
        parse.original = None;
        parse
    }

    pub fn data_types(&self) -> DataTypesIter<'_> {
        DataTypesIter {
            data_types: &self.data_types,
            offset: 0,
        }
    }

    pub fn data_types_ref(&self) -> Bytes {
        self.data_types.clone()
    }
}

#[derive(Debug)]
pub struct DataTypesIter<'a> {
    data_types: &'a Bytes,
    offset: usize,
}

impl DataTypesIter<'_> {
    pub fn len(&self) -> usize {
        (self.data_types.len() - size_of::<i16>()) / size_of::<i32>()
    }
}

impl Iterator for DataTypesIter<'_> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.offset * size_of::<i32>() + size_of::<i16>();
        self.offset += 1;
        let mut cursor = Cursor::new(self.data_types);
        cursor.advance(pos);

        if cursor.remaining() >= size_of::<i32>() {
            Some(cursor.get_i32())
        } else {
            None
        }
    }
}

impl FromBytes for Parse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'P');
        let _len = bytes.get_i32();
        let name_len = c_string_buf_len(&bytes);
        let name = bytes.split_to(name_len);
        let query_len = c_string_buf_len(&bytes);
        let query = bytes.split_to(query_len);
        let data_types = bytes;

        // Validate we got valid UTF-8.
        from_utf8(&name[0..name.len() - 1])?;
        from_utf8(&query[0..query.len() - 1])?;

        Ok(Self {
            name,
            query,
            data_types,
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

        payload.put(self.name.clone());
        payload.put(self.query.clone());
        payload.put(self.data_types.clone());

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
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_parse() {
        let parse = Parse::named("test", "SELECT $1");
        let b = parse.to_bytes().unwrap();
        assert_eq!(parse.len(), b.len());
    }

    #[test]
    fn test_parse_from_bytes() {
        let mut parse = Parse::named("__pgdog_1", "SELECT * FROM users");
        let mut data_types = BytesMut::new();
        data_types.put_i16(3);
        data_types.put_i32(1);
        data_types.put_i32(2);
        data_types.put_i32(3);
        parse.data_types = data_types.freeze();

        let iter = parse.data_types();
        assert_eq!(iter.len(), 3);
        for (i, v) in iter.enumerate() {
            assert_eq!(i as i32 + 1, v);
        }

        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");
        assert_eq!(&parse.query[..], b"SELECT * FROM users\0");
        assert_eq!(&parse.name[..], b"__pgdog_1\0");
        assert_eq!(parse.to_bytes().unwrap().len(), parse.len());

        let mut b = BytesMut::new();
        b.put_u8(b'P');
        b.put_i32(0); // Doesn't matter
        b.put(Bytes::from("__pgdog_1\0"));
        b.put(Bytes::from("SELECT * FROM users\0"));
        b.put_i16(0);
        let parse = Parse::from_bytes(b.freeze()).unwrap();
        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");
        assert_eq!(parse.data_types().len(), 0);

        assert!(Parse::new_anonymous("SELECT 1").anonymous());
    }
}
