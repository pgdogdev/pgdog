//! Module: wire_protocol::backend::parse_complete
//!
//! Provides parsing and serialization for the ParseComplete message ('1') in the protocol.
//!
//! - `ParseCompleteFrame`: represents the ParseComplete message indicating parse operation completion.
//! - `ParseCompleteError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ParseCompleteFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseCompleteFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ParseCompleteError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for ParseCompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseCompleteError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            ParseCompleteError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for ParseCompleteError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ParseCompleteFrame {
    type Error = ParseCompleteError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(ParseCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'1' {
            return Err(ParseCompleteError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(ParseCompleteError::UnexpectedLength(len));
        }

        if bytes.len() != 5 {
            return Err(ParseCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        Ok(ParseCompleteFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"1\0\0\0\x04"))
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
    fn serialize_parse_complete() {
        let frame = ParseCompleteFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"1\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_parse_complete() {
        let data = b"1\x00\x00\x00\x04";
        let frame = ParseCompleteFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = frame;
    }

    #[test]
    fn roundtrip_parse_complete() {
        let original = ParseCompleteFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = ParseCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"2\x00\x00\x00\x04";
        let err = ParseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, ParseCompleteError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"1\x00\x00\x00\x05";
        let err = ParseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, ParseCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"1\x00\x00\x00\x04\x00";
        let err = ParseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, ParseCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn short_data() {
        let data = b"1\x00\x00\x00";
        let err = ParseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, ParseCompleteError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
