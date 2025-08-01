//! Module: wire_protocol::backend::authentication_kerberos_v5
//!
//! Provides parsing and serialization for the AuthenticationKerberosV5 message ('R' with code 2) in the protocol.
//!
//! - `AuthenticationKerberosV5Frame`: represents the AuthenticationKerberosV5 message.
//! - `AuthenticationKerberosV5Error`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationKerberosV5Frame`.

use bytes::Bytes;
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationKerberosV5Frame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationKerberosV5Error {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationKerberosV5Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationKerberosV5Error::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationKerberosV5Error::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationKerberosV5Error::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationKerberosV5Error {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationKerberosV5Frame {
    type Error = AuthenticationKerberosV5Error;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationKerberosV5Error::UnexpectedLength(
                bytes.len() as u32
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationKerberosV5Error::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 8 {
            return Err(AuthenticationKerberosV5Error::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 2 {
            return Err(AuthenticationKerberosV5Error::UnexpectedAuthCode(code));
        }

        Ok(AuthenticationKerberosV5Frame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from_static(b"R\x00\x00\x00\x08\x00\x00\x00\x02"))
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
    fn serialize_auth_kerberos_v5() {
        let frame = AuthenticationKerberosV5Frame;
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x02";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_kerberos_v5() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x02";
        let frame = AuthenticationKerberosV5Frame::from_bytes(data).unwrap();
        let _ = frame;
    }

    #[test]
    fn roundtrip_auth_kerberos_v5() {
        let original = AuthenticationKerberosV5Frame;
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationKerberosV5Frame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x02";
        let err = AuthenticationKerberosV5Frame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationKerberosV5Error::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x09\x00\x00\x00\x02";
        let err = AuthenticationKerberosV5Frame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationKerberosV5Error::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x03";
        let err = AuthenticationKerberosV5Frame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationKerberosV5Error::UnexpectedAuthCode(3));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
