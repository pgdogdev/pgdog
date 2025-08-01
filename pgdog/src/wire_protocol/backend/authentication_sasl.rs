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
            AuthenticationSaslError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationSaslError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            AuthenticationSaslError::UnexpectedAuthCode(code) => {
                write!(f, "unexpected auth code: {code}")
            }
            AuthenticationSaslError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            AuthenticationSaslError::UnexpectedEof => write!(f, "unexpected EOF"),
            AuthenticationSaslError::InvalidLength => write!(f, "invalid length"),
            AuthenticationSaslError::MechanismError(e) => write!(f, "SASL mechanism error: {e}"),
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
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(AuthenticationSaslError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(AuthenticationSaslError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationSaslFrame {
    type Error = AuthenticationSaslError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 9 {
            return Err(AuthenticationSaslError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'R' {
            return Err(AuthenticationSaslError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len < 8 {
            return Err(AuthenticationSaslError::UnexpectedLength(len));
        }

        let code = bytes.get_i32();
        if code != 10 {
            return Err(AuthenticationSaslError::UnexpectedAuthCode(code));
        }

        let expected_body_len = (len - 8) as usize;
        let mut body_remaining = &bytes[..expected_body_len];
        let mut mechanisms = Vec::new();

        loop {
            let mech_str = read_cstr(&mut body_remaining)?;
            if mech_str.is_empty() {
                break;
            }
            let mech = SaslMechanism::from_str(mech_str)
                .map_err(AuthenticationSaslError::MechanismError)?;
            mechanisms.push(mech);
        }

        if !body_remaining.is_empty() {
            return Err(AuthenticationSaslError::InvalidLength);
        }

        Ok(AuthenticationSaslFrame { mechanisms })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.put_i32(10); // auth code

        for mech in &self.mechanisms {
            body.extend_from_slice(mech.as_str().as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminator

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'R');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
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
        let expected = b"R\x00\x00\x00\x18\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_auth_sasl() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
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
        let data = b"X\x00\x00\x00\x18\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"R\x00\x00\x00\x17\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_code() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0BSCRAM-SHA-256\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedAuthCode(11));
    }

    #[test]
    fn missing_terminator() {
        let data = b"R\x00\x00\x00\x17\x00\x00\x00\x0ASCRAM-SHA-256\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_terminator() {
        let data = b"R\x00\x00\x00\x19\x00\x00\x00\x0ASCRAM-SHA-256\x00\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::InvalidLength);
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[9] = 0xFF; // corrupt first byte of mechanism
        let err = AuthenticationSaslFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, AuthenticationSaslError::Utf8Error(_));
    }

    #[test]
    fn unsupported_mechanism() {
        let data = b"R\x00\x00\x00\x18\x00\x00\x00\x0AUNKNOWN-MECH\x00\x00";
        let err = AuthenticationSaslFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationSaslError::MechanismError(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
