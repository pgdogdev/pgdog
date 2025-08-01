//! Module: wire_protocol::backend::authentication_cleartext_password
//!
//! Provides parsing and serialization for the AuthenticationCleartextPassword message ('R' with code 3) in the protocol.
//!
//! - `AuthenticationCleartextPasswordFrame`: represents the AuthenticationCleartextPassword message.
//! - `AuthenticationCleartextPasswordError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationCleartextPasswordFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationCleartextPasswordFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationCleartextPasswordError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationCleartextPasswordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationCleartextPasswordError::UnexpectedTag(t) => {
                write!(f, "unexpected tag: {t:#X}")
            }
            AuthenticationCleartextPasswordError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationCleartextPasswordError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationCleartextPasswordError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationCleartextPasswordFrame {
    type Error = AuthenticationCleartextPasswordError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationCleartextPasswordError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationCleartextPasswordError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationCleartextPasswordError::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 3 {
            return Err(AuthenticationCleartextPasswordError::UnexpectedAuthCode(
                code,
            ));
        }

        Ok(AuthenticationCleartextPasswordFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x03"))
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
    fn serialize_auth_cleartext() {
        let frame = AuthenticationCleartextPasswordFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x03";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_cleartext() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x03";
        let frame = AuthenticationCleartextPasswordFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_auth_cleartext() {
        let original = AuthenticationCleartextPasswordFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationCleartextPasswordFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x03";
        let err = AuthenticationCleartextPasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationCleartextPasswordError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x03";
        let err = AuthenticationCleartextPasswordFrame::from_bytes(data).unwrap_err();
        matches!(
            err,
            AuthenticationCleartextPasswordError::UnexpectedLength(_)
        );
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x05";
        let err = AuthenticationCleartextPasswordFrame::from_bytes(data).unwrap_err();
        matches!(
            err,
            AuthenticationCleartextPasswordError::UnexpectedAuthCode(5)
        );
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
