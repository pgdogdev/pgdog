//! Module: wire_protocol::frontend::sync
//!
//! Provides parsing and serialization for the Sync message ('S') in the extended protocol.
//!
//! - `SyncFrame`: represents a Sync message sent by the client to synchronize after extended query messages.
//!
//! Implements `WireSerializable` for conversion between raw bytes and `SyncFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SyncError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            SyncError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for SyncError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for SyncFrame {
    type Error = SyncError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(SyncError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'S' {
            return Err(SyncError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(SyncError::UnexpectedLength(len));
        }

        Ok(SyncFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"S\0\0\0\x04"))
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
    fn serialize_sync() {
        let sync = SyncFrame;
        let bytes = sync.to_bytes().unwrap();
        let expected_bytes = Bytes::from_static(&[b'S', 0, 0, 0, 4]);
        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn deserialize_sync() {
        let data = &[b'S', 0, 0, 0, 4][..];
        let sync = SyncFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = sync;
    }

    #[test]
    fn roundtrip_sync() {
        let original = SyncFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = SyncFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = &[b'Q', 0, 0, 0, 4][..];
        let err = SyncFrame::from_bytes(data).unwrap_err();
        matches!(err, SyncError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = &[b'S', 0, 0, 0, 5][..];
        let err = SyncFrame::from_bytes(data).unwrap_err();
        matches!(err, SyncError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
