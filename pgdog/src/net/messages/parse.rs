//! Parse (F) message.
use crate::net::c_string_buf_len;
use bytes::BytesMut;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::mem;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use super::code;
use super::prelude::*;

use super::c_string_bytes;

/// Parse (F) message.
#[derive(Clone, Hash, Eq, PartialEq, Default)]
pub(crate) struct Parse {
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
    /// Length in bytes of the message.
    pub(crate) fn len(&self) -> usize {
        self.name.len() + self.query.len() + self.data_types.len() + 5
    }

    /// New anonymous prepared statement.
    pub(crate) fn new_anonymous(query: &str) -> Self {
        Self {
            name: Bytes::from("\0"),
            query: c_string_bytes(query),
            data_types: Bytes::copy_from_slice(&0i16.to_be_bytes()),
            original: None,
        }
    }

    /// New prepared statement.
    pub(crate) fn named(name: impl AsRef<str>, query: impl AsRef<str>) -> Self {
        Self {
            name: c_string_bytes(name.as_ref()),
            query: c_string_bytes(query.as_ref()),
            data_types: Bytes::copy_from_slice(&0i16.to_be_bytes()),
            original: None,
        }
    }

    /// Anonymous prepared statement.
    pub(crate) fn anonymous(&self) -> bool {
        self.name.len() == 1 // Just the null byte.
    }

    pub(crate) fn query(&self) -> &str {
        // SAFETY: We check that this is valid UTF-8 in Self::from_bytes.
        unsafe { from_utf8_unchecked(&self.query[0..self.query.len() - 1]) }
    }

    /// Get query reference.
    pub(crate) fn query_ref(&self) -> Bytes {
        self.query.clone()
    }

    pub(crate) fn name(&self) -> &str {
        // SAFETY: We check that this is valid UTF-8 in Self::from_bytes.
        unsafe { from_utf8_unchecked(&self.name[0..self.name.len() - 1]) }
    }

    /// Rename prepared statement, re-allocating it into its own memory space.
    pub(crate) fn renamed(&self, name: &str) -> Parse {
        // PERF: the allocation will create new allocation for inner data, that
        // won't pin any original buffers (allowing to modify them without new allocation)
        // and the new allocation memory size will be just limited by the actual data, not buffers
        Parse {
            name: c_string_bytes(name),
            query: Bytes::copy_from_slice(&self.query),
            data_types: Bytes::copy_from_slice(&self.data_types),
            original: None,
        }
    }

    /// Rename the prepared statement with minimal allocations.
    pub(crate) fn rename(&mut self, name: impl AsRef<str>) {
        self.name = c_string_bytes(name.as_ref());
        self.original = None;
    }

    /// Make this an anonymous Parse.
    pub(crate) fn anonymize(&mut self) {
        if !self.anonymous() {
            self.rename("");
        }
    }

    pub(crate) fn data_types_ref(&self) -> Bytes {
        self.data_types.clone()
    }

    /// Update the SQL for this prepared statement.
    pub(crate) fn set_query(&mut self, query: &str) {
        self.query = c_string_bytes(query);
        self.original = None;
    }

    /// Rewrite the data types of this message using the given mapping.
    /// Returns whether any data types were changed.
    pub(crate) fn rewrite_data_types(&mut self, mapping: &HashMap<u32, u32>) -> bool {
        if mapping.is_empty() {
            return false;
        }

        let mut rewritten = false;
        // FIXME: This will always copy the bytes, we should only copy on write
        let mut bytes = Cursor::new(BytesMut::from(mem::take(&mut self.data_types)));
        for _ in 0..bytes.get_u16() {
            let canonical_oid = bytes.get_u32();
            if let Some(&shard_oid) = mapping.get(&canonical_oid) {
                // Note: We can't use BufMut here, as that writes to the end of the
                // buffer, which will allocate. We just want to write to the
                // existing bytes.
                let pos = bytes.position() as usize;
                let oid_bytes = &mut bytes.get_mut()[(pos - 4)..pos];
                oid_bytes.copy_from_slice(&shard_oid.to_be_bytes());
                rewritten = true;
                self.original = None;
            }
        }
        self.data_types = bytes.into_inner().freeze();
        rewritten
    }

    pub(crate) fn with_data_types(&self, data_types: &[u32]) -> Self {
        let mut bytes = BytesMut::new();
        bytes.put_u16(data_types.len() as _);
        for &oid in data_types {
            bytes.put_u32(oid);
        }

        let mut this = self.clone();
        this.data_types = bytes.freeze();
        this
    }
}

impl FromBytes for Parse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'P');

        let len = bytes.get_i32() as usize;
        if bytes.remaining() + 4 < len {
            return Err(Error::UnexpectedEof);
        }

        let name_len = c_string_buf_len(&bytes);
        if name_len == 0 {
            return Err(Error::UnexpectedEof);
        }
        let name = bytes.split_to(name_len);

        let query_len = c_string_buf_len(&bytes);
        if query_len == 0 {
            return Err(Error::UnexpectedEof);
        }
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
    fn to_bytes(&self) -> Bytes {
        // Fast path when the contents haven't been changed.
        if let Some(ref original) = self.original {
            return original.clone();
        }

        let mut payload = Payload::named(self.code());
        payload.reserve(self.len());

        payload.put(self.name.clone());
        payload.put(self.query.clone());
        payload.put(self.data_types.clone());

        payload.freeze()
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
        let b = parse.to_bytes();
        assert_eq!(parse.len(), b.len());
    }

    #[test]
    fn test_parse_from_bytes() {
        let parse = Parse::named("__pgdog_1", "SELECT * FROM users");

        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");
        assert_eq!(&parse.query[..], b"SELECT * FROM users\0");
        assert_eq!(&parse.name[..], b"__pgdog_1\0");
        assert_eq!(parse.to_bytes().len(), parse.len());

        let mut b = BytesMut::new();
        b.put_u8(b'P');
        b.put_i32(0); // Doesn't matter
        b.put(Bytes::from("__pgdog_1\0"));
        b.put(Bytes::from("SELECT * FROM users\0"));
        b.put_i16(0);
        let parse = Parse::from_bytes(b.freeze()).unwrap();
        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");
        assert_eq!(parse.data_types_ref().get_i16(), 0);

        assert!(Parse::new_anonymous("SELECT 1").anonymous());
    }

    #[test]
    fn test_parse_rewrite_oids() {
        let mut parse = Parse::named("", "").with_data_types(&[10_001, 10_011, 10_091]);

        let mapping = [(10_001, 10_001), (10_011, 10_002), (10_091, 10_003)]
            .into_iter()
            .collect();
        assert!(parse.rewrite_data_types(&mapping));

        let expected = Parse::named("", "").with_data_types(&[10_001, 10_002, 10_003]);
        assert_eq!(parse, expected);

        let mut parse_without_custom_types = Parse::named("", "").with_data_types(&[1]);
        assert!(!parse_without_custom_types.rewrite_data_types(&mapping));
    }

    #[test]
    fn test_rewriting_oids_clears_original_bytes() {
        let mut parse = Parse::named("", "").with_data_types(&[10_001]);
        parse.original = Some(Bytes::new());
        let mapping = [(10_001, 10_002)].into_iter().collect();
        assert!(parse.rewrite_data_types(&mapping));
        assert!(!parse.to_bytes().is_empty());
    }

    #[test]
    fn test_renamed_owns_exact_memory() {
        let source = Parse::named("client_name", "SELECT $1").with_data_types(&[20, 25]);
        let renamed = source.renamed("__pgdog_7");

        assert_eq!(renamed.name(), "__pgdog_7");
        assert_eq!(renamed.query(), "SELECT $1");
        assert_eq!(renamed.data_types_ref(), source.data_types_ref());
        assert_ne!(renamed.query_ref().as_ptr(), source.query_ref().as_ptr());
        assert_ne!(
            renamed.data_types_ref().as_ptr(),
            source.data_types_ref().as_ptr()
        );

        let fresh = Parse::named("__pgdog_7", "SELECT $1").with_data_types(&[20, 25]);
        assert_eq!(renamed.to_bytes(), fresh.to_bytes());
        assert_eq!(renamed.to_bytes().len(), renamed.len());

        let round_trip = Parse::from_bytes(renamed.to_bytes()).unwrap();
        assert_eq!(round_trip.name(), renamed.name());
        assert_eq!(round_trip.query(), renamed.query());
        assert_eq!(round_trip.data_types_ref(), renamed.data_types_ref());
    }
}
