//! Module: wire_protocol::backend::authentication_scm_credential
//!
//! Provides parsing and serialization for the AuthenticationSCMCredential message ('R' with code 6) in the protocol.
//!
//! - `AuthenticationScmCredentialFrame`: represents the AuthenticationSCMCredential message requesting SCM credential authentication.
//! - `AuthenticationScmCredentialError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationScmCredentialFrame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationScmCredentialFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationScmCredentialError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthType(i32),
}

impl fmt::Display for AuthenticationScmCredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationScmCredentialError::UnexpectedTag(t) => {
                write!(f, "unexpected tag: {t:#X}")
            }
            AuthenticationScmCredentialError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationScmCredentialError::UnexpectedAuthType(t) => {
                write!(f, "unexpected auth type: {t}")
            }
        }
    }
}

impl StdError for AuthenticationScmCredentialError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationScmCredentialFrame {
    type Error = AuthenticationScmCredentialError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationScmCredentialError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationScmCredentialError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationScmCredentialError::UnexpectedLength(len));
        }

        if bytes.len() != 1 + len as usize {
            return Err(AuthenticationScmCredentialError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let auth_type = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if auth_type != 6 {
            return Err(AuthenticationScmCredentialError::UnexpectedAuthType(
                auth_type,
            ));
        }

        Ok(AuthenticationScmCredentialFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x06"))
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
    fn serialize_authentication_scm_credential() {
        let frame = AuthenticationScmCredentialFrame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x06";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_authentication_scm_credential() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x06";
        let frame = AuthenticationScmCredentialFrame::from_bytes(data).unwrap();
        // no state; just ensure no error
        let _ = frame;
    }

    #[test]
    fn roundtrip_authentication_scm_credential() {
        let original = AuthenticationScmCredentialFrame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationScmCredentialFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x06";
        let err = AuthenticationScmCredentialFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationScmCredentialError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x06";
        let err = AuthenticationScmCredentialFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationScmCredentialError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x06\x00";
        let err = AuthenticationScmCredentialFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationScmCredentialError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_type() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x07";
        let err = AuthenticationScmCredentialFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationScmCredentialError::UnexpectedAuthType(7));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
