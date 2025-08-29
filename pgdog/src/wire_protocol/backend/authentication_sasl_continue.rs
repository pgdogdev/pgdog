//! Module: wire_protocol::backend::authentication_sasl_continue
//!
//! Provides parsing and serialization for the AuthenticationSASLContinue message ('R' with code 11) in the protocol.
//!
//! - `AuthenticationSaslContinueFrame`: represents the AuthenticationSASLContinue message with SASL continuation data.
//! - `AuthenticationSaslContinueError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationSaslContinueFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationSaslContinueFrame<'a> {
    pub sasl_data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationSaslContinueError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationSaslContinueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationSaslContinueError::UnexpectedTag(t) => {
                write!(f, "unexpected tag: {t:#X}")
            }
            AuthenticationSaslContinueError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationSaslContinueError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationSaslContinueError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationSaslContinueFrame<'a> {
    type Error = AuthenticationSaslContinueError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationSaslContinueError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationSaslContinueError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if (len as usize) + 1 != bytes.len() {
            return Err(AuthenticationSaslContinueError::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 11 {
            return Err(AuthenticationSaslContinueError::UnexpectedAuthCode(code));
        }

        let sasl_data = &bytes[9..];

        Ok(AuthenticationSaslContinueFrame { sasl_data })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = 4 + self.sasl_data.len();
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'R');
        buf.put_u32(total_len as u32);
        buf.put_i32(11);
        buf.extend_from_slice(self.sasl_data);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self.sasl_data.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> AuthenticationSaslContinueFrame<'a> {
        AuthenticationSaslContinueFrame {
            sasl_data: b"r=some_nonce,s=salt,i=4096",
        }
    }

    #[test]
    fn serialize_auth_sasl_continue() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        // length = 4 (len) + 4 (code) + 26 (data) = 34 => 0x22
        let expected = b"R\x00\x00\x00\x22\x00\x00\x00\x0Br=some_nonce,s=salt,i=4096";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_sasl_continue() {
        let data = b"R\x00\x00\x00\x22\x00\x00\x00\x0Br=some_nonce,s=salt,i=4096";
        let frame = AuthenticationSaslContinueFrame::from_bytes(data).unwrap();
        assert_eq!(frame.sasl_data, b"r=some_nonce,s=salt,i=4096");
    }

    #[test]
    fn roundtrip_auth_sasl_continue() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationSaslContinueFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.sasl_data, original.sasl_data);
    }

    #[test]
    fn empty_data() {
        let frame = AuthenticationSaslContinueFrame { sasl_data: b"" };
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x0B";
        assert_eq!(bytes.as_ref(), expected);

        let decoded = AuthenticationSaslContinueFrame::from_bytes(bytes.as_ref()).unwrap();
        assert!(decoded.sasl_data.is_empty());
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x1F\x00\x00\x00\x0Br=some_nonce,s=salt,i=4096";
        let err = AuthenticationSaslContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslContinueError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x20\x00\x00\x00\x0Br=some_nonce,s=salt,i=4096";
        let err = AuthenticationSaslContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslContinueError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x1F\x00\x00\x00\x0Ar=some_nonce,s=salt,i=4096";
        let err = AuthenticationSaslContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslContinueError::UnexpectedAuthCode(10));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
