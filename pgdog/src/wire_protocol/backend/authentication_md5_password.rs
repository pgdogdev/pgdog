//! Module: wire_protocol::backend::authentication_md5_password
//!
//! Provides parsing and serialization for the AuthenticationMD5Password message ('R' with code 5) in the protocol.
//!
//! - `AuthenticationMd5PasswordFrame`: represents the AuthenticationMD5Password message with a 4-byte salt.
//! - `AuthenticationMd5PasswordError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationMd5PasswordFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticationMd5PasswordFrame {
    pub salt: [u8; 4],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationMd5PasswordError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthType(i32),
}

impl fmt::Display for AuthenticationMd5PasswordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationMd5PasswordError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationMd5PasswordError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationMd5PasswordError::UnexpectedAuthType(t) => {
                write!(f, "unexpected auth type: {t}")
            }
        }
    }
}

impl StdError for AuthenticationMd5PasswordError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationMd5PasswordFrame {
    type Error = AuthenticationMd5PasswordError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 13 {
            return Err(AuthenticationMd5PasswordError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationMd5PasswordError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 12 {
            return Err(AuthenticationMd5PasswordError::UnexpectedLength(len));
        }

        if bytes.len() != 1 + len as usize {
            return Err(AuthenticationMd5PasswordError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let auth_type = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if auth_type != 5 {
            return Err(AuthenticationMd5PasswordError::UnexpectedAuthType(
                auth_type,
            ));
        }

        let salt = [bytes[9], bytes[10], bytes[11], bytes[12]];

        Ok(AuthenticationMd5PasswordFrame { salt })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(13);
        buf.put_u8(b'R');
        buf.put_u32(12);
        buf.put_i32(5);
        buf.extend_from_slice(&self.salt);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        8
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> AuthenticationMd5PasswordFrame {
        AuthenticationMd5PasswordFrame {
            salt: [0x01, 0x02, 0x03, 0x04],
        }
    }

    #[test]
    fn serialize_authentication_md5_password() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x0C\x00\x00\x00\x05\x01\x02\x03\x04";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_authentication_md5_password() {
        let data = b"R\x00\x00\x00\x0C\x00\x00\x00\x05\x01\x02\x03\x04";
        let frame = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap();
        assert_eq!(frame.salt, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn roundtrip_authentication_md5_password() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationMd5PasswordFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x0C\x00\x00\x00\x05\x01\x02\x03\x04";
        let err = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationMd5PasswordError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x0D\x00\x00\x00\x05\x01\x02\x03\x04";
        let err = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationMd5PasswordError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"R\x00\x00\x00\x0C\x00\x00\x00\x05\x01\x02\x03\x04\x00";
        let err = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationMd5PasswordError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_type() {
        let data = b"R\x00\x00\x00\x0C\x00\x00\x00\x07\x01\x02\x03\x04";
        let err = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationMd5PasswordError::UnexpectedAuthType(7));
    }

    #[test]
    fn short_data() {
        let data = b"R\x00\x00\x00\x0C\x00\x00\x00\x05\x01\x02\x03";
        let err = AuthenticationMd5PasswordFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationMd5PasswordError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
