//! Module: wire_protocol::backend::authentication_ok
//!
//! Provides parsing and serialization for the AuthenticationOK message ('R' with code 0) in the protocol.
//!
//! - `AuthenticationOkFrame`: represents the AuthenticationOK message.
//! - `AuthenticationOkError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationOkFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationOkFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationOkError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationOkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationOkError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationOkError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            AuthenticationOkError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationOkError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationOkFrame {
    type Error = AuthenticationOkError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationOkError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationOkError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationOkError::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 0 {
            return Err(AuthenticationOkError::UnexpectedAuthCode(code));
        }

        Ok(AuthenticationOkFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x00"))
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
    fn serialize_auth_ok() {
        let frame = AuthenticationOkFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_ok() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x00";
        let frame = AuthenticationOkFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_auth_ok() {
        let original = AuthenticationOkFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationOkFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x00";
        let err = AuthenticationOkFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationOkError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x00";
        let err = AuthenticationOkFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationOkError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x03";
        let err = AuthenticationOkFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationOkError::UnexpectedAuthCode(3));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
