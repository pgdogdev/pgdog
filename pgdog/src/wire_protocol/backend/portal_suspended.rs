//! Module: wire_protocol::backend::portal_suspended
//!
//! Provides parsing and serialization for the PortalSuspended message ('s') in the protocol.
//!
//! - `PortalSuspendedFrame`: represents the PortalSuspended message.
//! - `PortalSuspendedError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `PortalSuspendedFrame`.

use bytes::Bytes;

use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------

// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortalSuspendedFrame;

// -----------------------------------------------------------------------------

// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum PortalSuspendedError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for PortalSuspendedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortalSuspendedError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            PortalSuspendedError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for PortalSuspendedError {}

// -----------------------------------------------------------------------------

// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for PortalSuspendedFrame {
    type Error = PortalSuspendedError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(PortalSuspendedError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b's' {
            return Err(PortalSuspendedError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 4 {
            return Err(PortalSuspendedError::UnexpectedLength(len));
        }

        Ok(PortalSuspendedFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"s\x00\x00\x00\x04"))
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
    fn serialize_portal_suspended() {
        let frame = PortalSuspendedFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"s\x00\x00\x00\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_portal_suspended() {
        let data = b"s\x00\x00\x00\x04";
        let frame = PortalSuspendedFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_portal_suspended() {
        let original = PortalSuspendedFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = PortalSuspendedFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x04";
        let err = PortalSuspendedFrame::from_bytes(data).unwrap_err();
        matches!(err, PortalSuspendedError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"s\x00\x00\x00\x05";
        let err = PortalSuspendedFrame::from_bytes(data).unwrap_err();
        matches!(err, PortalSuspendedError::UnexpectedLength(5));
    }
}

// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
