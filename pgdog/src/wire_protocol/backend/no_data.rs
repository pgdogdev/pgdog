//! Module: wire_protocol::backend::no_data
//!
//! Provides parsing and serialization for the NoData message ('n') in the protocol.
//!
//! - `NoDataFrame`: represents the NoData message.
//! - `NoDataError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `NoDataFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoDataFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum NoDataError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for NoDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoDataError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            NoDataError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for NoDataError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for NoDataFrame {
    type Error = NoDataError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(NoDataError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'n' {
            return Err(NoDataError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(NoDataError::UnexpectedLength(len));
        }

        Ok(NoDataFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"n\x00\x00\x00\x04"))
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
    fn serialize_no_data() {
        let frame = NoDataFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"n\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_no_data() {
        let data = b"n\x00\x00\x00\x04";
        let frame = NoDataFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_no_data() {
        let original = NoDataFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = NoDataFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x04";
        let err = NoDataFrame::from_bytes(data).unwrap_err();
        matches!(err, NoDataError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"n\x00\x00\x00\x05";
        let err = NoDataFrame::from_bytes(data).unwrap_err();
        matches!(err, NoDataError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
