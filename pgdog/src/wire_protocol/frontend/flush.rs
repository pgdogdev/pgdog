//! Module: wire_protocol::frontend::flush
//!
//! Provides parsing and serialization for the Flush message ('H') in the
//! extended protocol.
//!
//! - `FlushFrame`: represents a Flush message sent by the client to force the
//!   backend to deliver any pending results.
//!
//! Implements `WireSerializable` for conversion between raw bytes and
//! `FlushFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- Message ---------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum FlushError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlushError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            FlushError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for FlushError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for FlushFrame {
    type Error = FlushError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(FlushError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'H' {
            return Err(FlushError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(FlushError::UnexpectedLength(len));
        }

        Ok(FlushFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"H\0\0\0\x04"))
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
    fn serialize_flush() {
        let flush = FlushFrame;
        let bytes = flush.to_bytes().unwrap();
        let expected = Bytes::from_static(&[b'H', 0, 0, 0, 4]);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn deserialize_flush() {
        let data = &[b'H', 0, 0, 0, 4][..];
        let _ = FlushFrame::from_bytes(data).unwrap();
    }

    #[test]
    fn roundtrip_flush() {
        let original = FlushFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = FlushFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = &[b'Q', 0, 0, 0, 4][..];
        let err = FlushFrame::from_bytes(data).unwrap_err();
        matches!(err, FlushError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = &[b'H', 0, 0, 0, 5][..];
        let err = FlushFrame::from_bytes(data).unwrap_err();
        matches!(err, FlushError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
