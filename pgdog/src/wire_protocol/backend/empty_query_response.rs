//! Module: wire_protocol::backend::empty_query_response
//!
//! Provides parsing and serialization for the EmptyQueryResponse message ('I') in the protocol.
//!
//! - `EmptyQueryResponseFrame`: represents the EmptyQueryResponse message.
//! - `EmptyQueryResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `EmptyQueryResponseFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyQueryResponseFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum EmptyQueryResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for EmptyQueryResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmptyQueryResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            EmptyQueryResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for EmptyQueryResponseError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for EmptyQueryResponseFrame {
    type Error = EmptyQueryResponseError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(EmptyQueryResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'I' {
            return Err(EmptyQueryResponseError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(EmptyQueryResponseError::UnexpectedLength(len));
        }

        Ok(EmptyQueryResponseFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"I\x00\x00\x00\x04"))
    }

    fn body_size(&self) -> usize {
        0
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_empty_query_response() {
        let frame = EmptyQueryResponseFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"I\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_empty_query_response() {
        let data = b"I\x00\x00\x00\x04";
        let frame = EmptyQueryResponseFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_empty_query_response() {
        let original = EmptyQueryResponseFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = EmptyQueryResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x04";
        let err = EmptyQueryResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, EmptyQueryResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"I\x00\x00\x00\x05";
        let err = EmptyQueryResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, EmptyQueryResponseError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
