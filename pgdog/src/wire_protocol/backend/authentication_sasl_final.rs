//! Module: wire_protocol::backend::authentication_sasl_final
//!
//! Provides parsing and serialization for the AuthenticationSASLFinal message ('R' with code 12) in the protocol.
//!
//! - `AuthenticationSaslFinalFrame`: represents the AuthenticationSASLFinal message with SASL final data.
//! - `AuthenticationSaslFinalError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationSaslFinalFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationSaslFinalFrame<'a> {
    pub sasl_data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationSaslFinalError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
}

impl fmt::Display for AuthenticationSaslFinalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationSaslFinalError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationSaslFinalError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationSaslFinalError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
        }
    }
}

impl StdError for AuthenticationSaslFinalError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationSaslFinalFrame<'a> {
    type Error = AuthenticationSaslFinalError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationSaslFinalError::UnexpectedLength(
                bytes.len() as u32
            ));
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationSaslFinalError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if (len as usize) + 1 != bytes.len() {
            return Err(AuthenticationSaslFinalError::UnexpectedLength(len));
        }

        let code = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if code != 12 {
            return Err(AuthenticationSaslFinalError::UnexpectedAuthCode(code));
        }

        let sasl_data = &bytes[9..];

        Ok(AuthenticationSaslFinalFrame { sasl_data })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = 4 + self.sasl_data.len();
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'R');
        buf.put_u32(total_len as u32);
        buf.put_i32(12);
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

    fn make_frame<'a>() -> AuthenticationSaslFinalFrame<'a> {
        AuthenticationSaslFinalFrame {
            sasl_data: b"v=some_signature",
        }
    }

    #[test]
    fn serialize_auth_sasl_final() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        // length = 24 (0x18)
        let expected = b"R\x00\x00\x00\x18\x00\x00\x00\x0Cv=some_signature";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_sasl_final() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0Cv=some_signature";
        let frame = AuthenticationSaslFinalFrame::from_bytes(data).unwrap();
        assert_eq!(frame.sasl_data, b"v=some_signature");
    }

    #[test]
    fn roundtrip_auth_sasl_final() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationSaslFinalFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.sasl_data, original.sasl_data);
    }

    #[test]
    fn empty_data() {
        let frame = AuthenticationSaslFinalFrame { sasl_data: b"" };
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x0C";
        assert_eq!(bytes.as_ref(), expected);

        let decoded = AuthenticationSaslFinalFrame::from_bytes(bytes.as_ref()).unwrap();
        assert!(decoded.sasl_data.is_empty());
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x14\x00\x00\x00\x0Cv=some_signature";
        let err = AuthenticationSaslFinalFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslFinalError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x15\x00\x00\x00\x0Cv=some_signature";
        let err = AuthenticationSaslFinalFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslFinalError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x14\x00\x00\x00\x0Bv=some_signature";
        let err = AuthenticationSaslFinalFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslFinalError::UnexpectedAuthCode(11));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
