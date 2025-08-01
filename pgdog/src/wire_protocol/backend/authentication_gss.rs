//! Module: wire_protocol::backend::authentication_gss
//!
//! Provides parsing and serialization for the AuthenticationGSS message ('R' with code 7) in the protocol.
//!
//! - `AuthenticationGssFrame`: represents the AuthenticationGSS message requesting GSSAPI authentication.
//! - `AuthenticationGssError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationGssFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationGssFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationGssError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthType(i32),
}

impl fmt::Display for AuthenticationGssError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationGssError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationGssError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            AuthenticationGssError::UnexpectedAuthType(t) => write!(f, "unexpected auth type: {t}"),
        }
    }
}

impl StdError for AuthenticationGssError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationGssFrame {
    type Error = AuthenticationGssError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(AuthenticationGssError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationGssError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationGssError::UnexpectedLength(len));
        }

        if bytes.len() != 1 + len as usize {
            return Err(AuthenticationGssError::UnexpectedLength(bytes.len() as u32));
        }

        let auth_type = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if auth_type != 7 {
            return Err(AuthenticationGssError::UnexpectedAuthType(auth_type));
        }

        Ok(AuthenticationGssFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x07"))
    }

    fn body_size(&self) -> usize {
        4
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_authentication_gss() {
        let frame = AuthenticationGssFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x07";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_authentication_gss() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x07";
        let frame = AuthenticationGssFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = frame;
    }

    #[test]
    fn roundtrip_authentication_gss() {
        let original = AuthenticationGssFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationGssFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x07";
        let err = AuthenticationGssFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x07";
        let err = AuthenticationGssFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x07\x00";
        let err = AuthenticationGssFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_type() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x00";
        let err = AuthenticationGssFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssError::UnexpectedAuthType(0));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
