//! Module: wire_protocol::backend::authentication_sspi
//!
//! Provides parsing and serialization for the AuthenticationSSPI message ('R' with code 9) in the protocol.
//!
//! - `AuthenticationSspiFrame`: represents the AuthenticationSSPI message.
//! - `AuthenticationSspiError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationSspiFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationSspiFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationSspiError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationSspiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationSspiError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationSspiError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            AuthenticationSspiError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationSspiError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationSspiFrame {
    type Error = AuthenticationSspiError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationSspiError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationSspiError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationSspiError::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 9 {
            return Err(AuthenticationSspiError::UnexpectedAuthCode(code));
        }

        Ok(AuthenticationSspiFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x09"))
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
    fn serialize_auth_sspi() {
        let frame = AuthenticationSspiFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x09";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_sspi() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x09";
        let frame = AuthenticationSspiFrame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_auth_sspi() {
        let original = AuthenticationSspiFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationSspiFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x09";
        let err = AuthenticationSspiFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSspiError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x09";
        let err = AuthenticationSspiFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSspiError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x0A";
        let err = AuthenticationSspiFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSspiError::UnexpectedAuthCode(10));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
