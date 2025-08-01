//! Module: wire_protocol::backend::close_complete
//!
//! Provides parsing and serialization for the CloseComplete message ('3') in the protocol.
//!
//! - `CloseCompleteFrame`: represents the CloseComplete message indicating close operation completion.
//! - `CloseCompleteError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CloseCompleteFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseCompleteFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CloseCompleteError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for CloseCompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloseCompleteError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CloseCompleteError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for CloseCompleteError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CloseCompleteFrame {
    type Error = CloseCompleteError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(CloseCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'3' {
            return Err(CloseCompleteError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(CloseCompleteError::UnexpectedLength(len));
        }

        if bytes.len() != 5 {
            return Err(CloseCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        Ok(CloseCompleteFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"3\0\0\0\x04"))
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
    fn serialize_close_complete() {
        let frame = CloseCompleteFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"3\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_close_complete() {
        let data = b"3\x00\x00\x00\x04";
        let frame = CloseCompleteFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = frame;
    }

    #[test]
    fn roundtrip_close_complete() {
        let original = CloseCompleteFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = CloseCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"2\x00\x00\x00\x04";
        let err = CloseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CloseCompleteError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"3\x00\x00\x00\x05";
        let err = CloseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CloseCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"3\x00\x00\x00\x04\x00";
        let err = CloseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CloseCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn short_data() {
        let data = b"3\x00\x00\x00";
        let err = CloseCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CloseCompleteError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
