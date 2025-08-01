//! Module: wire_protocol::frontend::terminate
//!
//! Provides parsing and serialization for the Terminate message ('X') in the extended protocol.
//!
//! - `Terminate`: represents a Terminate message sent by the client to close the connection.
//!
//! Implements `WireSerializable` for conversion between raw bytes and `Terminate`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminateFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum TerminateError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for TerminateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TerminateError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            TerminateError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for TerminateError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for TerminateFrame {
    type Error = TerminateError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(TerminateError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'X' {
            return Err(TerminateError::UnexpectedTag(tag));
        }
        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(TerminateError::UnexpectedLength(len));
        }
        Ok(TerminateFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"X\0\0\0\x04"))
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
    fn serialize_terminate() {
        let term = TerminateFrame;
        let bytes = term.to_bytes().unwrap();
        let expected_bytes = Bytes::from_static(&[b'X', 0, 0, 0, 4]);
        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn deserialize_terminate() {
        let data = &[b'X', 0, 0, 0, 4][..];
        let term = TerminateFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = term;
    }

    #[test]
    fn roundtrip_terminate() {
        let original = TerminateFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = TerminateFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = &[b'Q', 0, 0, 0, 4][..];
        let err = TerminateFrame::from_bytes(data).unwrap_err();
        matches!(err, TerminateError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = &[b'X', 0, 0, 0, 5][..];
        let err = TerminateFrame::from_bytes(data).unwrap_err();
        matches!(err, TerminateError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
