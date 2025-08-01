//! Module: wire_protocol::frontend::query
//!
//! Provides parsing and serialization for the Query message ('Q') in the simple query protocol.
//!
//! - `QueryFrame`: represents a Query message with the SQL query string.
//! - `QueryError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `QueryFrame`.

use bytes::{BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct QueryFrame<'a> {
    pub query: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum QueryError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            QueryError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            QueryError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            QueryError::UnexpectedEof => write!(f, "unexpected EOF"),
        }
    }
}

impl StdError for QueryError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            QueryError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(bytes: &'a [u8]) -> Result<(&'a str, usize), QueryError> {
    let nul = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or(QueryError::UnexpectedEof)?;
    let raw = &bytes[..nul];
    let s = str::from_utf8(raw).map_err(QueryError::Utf8Error)?;
    Ok((s, nul + 1))
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for QueryFrame<'a> {
    type Error = QueryError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(QueryError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'Q' {
            return Err(QueryError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(QueryError::UnexpectedLength(len));
        }

        let (query, consumed) = read_cstr(&bytes[5..])?;
        if consumed != bytes.len() - 5 {
            return Err(QueryError::UnexpectedLength(len));
        }

        Ok(QueryFrame { query })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = self.query.len() + 1;
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'Q');
        buf.put_u32(total_len as u32);
        buf.extend_from_slice(self.query.as_bytes());
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.query.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> QueryFrame<'static> {
        QueryFrame { query: "SELECT 1" }
    }

    #[test]
    fn serialize_query() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"Q\x00\x00\x00\x0DSELECT 1\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_query() {
        let data = b"Q\x00\x00\x00\x0DSELECT 1\x00";
        let frame = QueryFrame::from_bytes(data).unwrap();
        assert_eq!(frame.query, "SELECT 1");
    }

    #[test]
    fn roundtrip_query() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = QueryFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.query, original.query);
    }

    #[test]
    fn invalid_tag() {
        let data = b"P\x00\x00\x00\x0ASELECT 1\x00";
        let err = QueryFrame::from_bytes(data).unwrap_err();
        matches!(err, QueryError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"Q\x00\x00\x00\x0BSELECT 1\x00";
        let err = QueryFrame::from_bytes(data).unwrap_err();
        matches!(err, QueryError::UnexpectedLength(_));
    }

    #[test]
    fn missing_null_terminator() {
        let data = b"Q\x00\x00\x00\x0ASELECT 1";
        let err = QueryFrame::from_bytes(data).unwrap_err();
        matches!(err, QueryError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_null() {
        let data = b"Q\x00\x00\x00\x0ASELECT 1\x00extra";
        let err = QueryFrame::from_bytes(data).unwrap_err();
        matches!(err, QueryError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[5] = 0xFF; // corrupt first byte
        let err = QueryFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, QueryError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
