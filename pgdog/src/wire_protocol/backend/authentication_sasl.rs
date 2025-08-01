//! Module: wire_protocol::backend::authentication_sasl
//!
//! Provides parsing and serialization for the AuthenticationSASL message ('R' with code 10) in the protocol.
//!
//! - `AuthenticationSaslFrame`: represents the AuthenticationSASL message with a list of supported mechanisms.
//! - `AuthenticationSaslError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationSaslFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::shared_property_types::{SaslMechanism, SaslMechanismError};
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationSaslFrame {
    pub mechanisms: Vec<SaslMechanism>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationSaslError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthCode(i32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
    MechanismError(SaslMechanismError),
}

impl fmt::Display for AuthenticationSaslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationSaslError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            AuthenticationSaslError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {}", len)
            }
            AuthenticationSaslError::UnexpectedAuthCode(c) => {
                write!(f, "unexpected auth code: {}", c)
            }
            AuthenticationSaslError::Utf8Error(e) => write!(f, "UTF-8 error: {}", e),
            AuthenticationSaslError::UnexpectedEof => write!(f, "unexpected EOF"),
            AuthenticationSaslError::InvalidLength => write!(f, "invalid length"),
            AuthenticationSaslError::MechanismError(e) => write!(f, "SASL mechanism error: {}", e),
        }
    }
}

impl StdError for AuthenticationSaslError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            AuthenticationSaslError::Utf8Error(e) => Some(e),
            AuthenticationSaslError::MechanismError(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, AuthenticationSaslError> {
    if let Some(pos) = buf.iter().position(|&b| b == 0) {
        let (raw, rest) = buf.split_at(pos);
        *buf = &rest[1..];
        return str::from_utf8(raw).map_err(AuthenticationSaslError::Utf8Error);
    }
    Err(AuthenticationSaslError::UnexpectedEof)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationSaslFrame {
    type Error = AuthenticationSaslError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 10 {
            return Err(AuthenticationSaslError::UnexpectedLength(bytes.len() as u32));
        }
        if bytes[0] != b'R' {
            return Err(AuthenticationSaslError::UnexpectedTag(bytes[0]));
        }
        let mut len_buf = &bytes[1..5];
        let len = len_buf.get_u32();
        if (bytes.len() - 1) != len as usize {
            return Err(AuthenticationSaslError::UnexpectedLength(len));
        }
        let mut rest = &bytes[5..];
        let code = rest.get_i32();
        if code != 10 {
            return Err(AuthenticationSaslError::UnexpectedAuthCode(code));
        }
        let mut body = &rest[..(len as usize - 8)];
        let mut mechanisms = Vec::new();
        loop {
            let mech_str = read_cstr(&mut body)?;
            if mech_str.is_empty() {
                break;
            }
            let mech = SaslMechanism::from_str(mech_str)
                .map_err(AuthenticationSaslError::MechanismError)?;
            mechanisms.push(mech);
        }
        if !body.is_empty() {
            return Err(AuthenticationSaslError::InvalidLength);
        }
        Ok(AuthenticationSaslFrame { mechanisms })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::new();
        body.put_i32(10);
        for mech in &self.mechanisms {
            body.extend_from_slice(mech.as_str().as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);

        let mut buf = BytesMut::with_capacity(1 + 4 + body.len());
        buf.put_u8(b'R');
        buf.put_u32((body.len() + 4) as u32);
        buf.extend_from_slice(&body);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self
            .mechanisms
            .iter()
            .map(|m| m.as_str().len() + 1)
            .sum::<usize>()
            + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> AuthenticationSaslFrame {
        AuthenticationSaslFrame {
            mechanisms: vec![SaslMechanism::ScramSha256],
        }
    }

    fn make_multi_frame() -> AuthenticationSaslFrame {
        AuthenticationSaslFrame {
            mechanisms: vec![SaslMechanism::ScramSha256, SaslMechanism::ScramSha256Plus],
        }
    }

    #[test]
    fn serialize_auth_sasl() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x17\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_sasl() {
        let data = b"R\x00\x00\x00\x17\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        let frame = AuthenticationSaslFrame::from_bytes(data).unwrap();
        assert_eq!(frame.mechanisms, vec![SaslMechanism::ScramSha256]);
    }

    #[test]
    fn roundtrip_auth_sasl() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationSaslFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.mechanisms, original.mechanisms);
    }

    #[test]
    fn roundtrip_multi_mechanisms() {
        let original = make_multi_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationSaslFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.mechanisms, original.mechanisms);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x17\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x17\x00\x00\x00\x0BSCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedAuthCode(11));
    }

    #[test]
    fn missing_terminator() {
        let data = b"R\x00\x00\x00\x16\x00\x00\x00\x0ASCRAM-SHA-256\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_terminator() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::InvalidLength);
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[9] = 0xFF;
        let err = AuthenticationSaslFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, AuthenticationSaslError::Utf8Error(_));
    }

    #[test]
    fn unsupported_mechanism() {
        let data = b"R\x00\x00\x00\x16\x00\x00\x00\x0AUNKNOWN-MECH\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::MechanismError(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
