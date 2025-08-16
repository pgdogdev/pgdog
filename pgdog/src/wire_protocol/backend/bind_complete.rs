//! Module: wire_protocol::backend::bind_complete
//!
//! Provides parsing and serialization for the BindComplete message ('2') in the protocol.
//!
//! - `BindCompleteFrame`: represents the BindComplete message indicating bind operation completion.
//! - `BindCompleteError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `BindCompleteFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindCompleteFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum BindCompleteError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for BindCompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindCompleteError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            BindCompleteError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for BindCompleteError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for BindCompleteFrame {
    type Error = BindCompleteError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(BindCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        // tag must be '2'
        if bytes[0] != b'2' {
            return Err(BindCompleteError::UnexpectedTag(bytes[0]));
        }

        // length field must be exactly 4 (no body)
        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(BindCompleteError::UnexpectedLength(len));
        }

        // reject any extra or missing bytes beyond the 5-byte header
        if bytes.len() != 1 + len as usize {
            return Err(BindCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        Ok(BindCompleteFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"2\0\0\0\x04"))
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
    fn serialize_bind_complete() {
        let frame = BindCompleteFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"2\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_bind_complete() {
        let data = b"2\x00\x00\x00\x04";
        let _ = BindCompleteFrame::from_bytes(data).unwrap();
    }

    #[test]
    fn roundtrip_bind_complete() {
        let original = BindCompleteFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = BindCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"3\x00\x00\x00\x04";
        let err = BindCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, BindCompleteError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"2\x00\x00\x00\x05";
        let err = BindCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, BindCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"2\x00\x00\x00\x04\x00";
        let err = BindCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, BindCompleteError::UnexpectedLength(6));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
