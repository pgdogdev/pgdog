//! Module: wire_protocol::frontend::copy_done
//!
//! Provides parsing and serialization for the CopyDone message ('c') in the extended protocol.
//!
//! - `CopyDoneFrame`: represents a CopyDone message sent by the client to indicate the end of COPY data.
//!
//! Implements `WireSerializable` for conversion between raw bytes and `CopyDoneFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyDoneFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyDoneError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for CopyDoneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyDoneError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CopyDoneError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for CopyDoneError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CopyDoneFrame {
    type Error = CopyDoneError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(CopyDoneError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'c' {
            return Err(CopyDoneError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(CopyDoneError::UnexpectedLength(len));
        }

        Ok(CopyDoneFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"c\0\0\0\x04"))
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
    fn serialize_copy_done() {
        let copy_done = CopyDoneFrame;
        let bytes = copy_done.to_bytes().unwrap();
        let expected_bytes = Bytes::from_static(&[b'c', 0, 0, 0, 4]);
        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn deserialize_copy_done() {
        let data = &[b'c', 0, 0, 0, 4][..];
        let copy_done = CopyDoneFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = copy_done;
    }

    #[test]
    fn roundtrip_copy_done() {
        let original = CopyDoneFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyDoneFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = &[b'Q', 0, 0, 0, 4][..];
        let err = CopyDoneFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyDoneError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = &[b'c', 0, 0, 0, 5][..];
        let err = CopyDoneFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyDoneError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
