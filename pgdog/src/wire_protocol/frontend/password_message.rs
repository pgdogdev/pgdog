//! Module: wire_protocol::frontend::password
//!
//! Provides parsing and serialization for the PasswordMessage message ('p') used in password authentication.
//!
//! This message is sent by the client in response to AuthenticationCleartextPassword, AuthenticationMD5Password, etc.
//!
//! - `PasswordMessageFrame`: represents a PasswordMessage message carrying the password as a null-terminated string.
//! - `PasswordMessageError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `PasswordMessageFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PasswordMessageFrame<'a> {
    pub password: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum PasswordMessageError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    ExtraDataAfterNull,
}

impl fmt::Display for PasswordMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PasswordMessageError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            PasswordMessageError::UnexpectedLength(len) => write!(f, "unexpected length: {}", len),
            PasswordMessageError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            PasswordMessageError::UnexpectedEof => write!(f, "unexpected EOF"),
            PasswordMessageError::ExtraDataAfterNull => {
                write!(f, "extra data after null terminator")
            }
        }
    }
}

impl StdError for PasswordMessageError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            PasswordMessageError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(bytes: &'a [u8]) -> Result<(&'a str, usize), PasswordMessageError> {
    let nul_pos = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or(PasswordMessageError::UnexpectedEof)?;
    let raw = &bytes[..nul_pos];
    let s = str::from_utf8(raw).map_err(PasswordMessageError::Utf8Error)?;
    Ok((s, nul_pos + 1))
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for PasswordMessageFrame<'a> {
    type Error = PasswordMessageError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(PasswordMessageError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'p' {
            return Err(PasswordMessageError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(PasswordMessageError::UnexpectedLength(len));
        }

        let (password, consumed) = read_cstr(&bytes[5..])?;
        if consumed != bytes.len() - 5 {
            return Err(PasswordMessageError::ExtraDataAfterNull);
        }

        Ok(PasswordMessageFrame { password })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = self.password.len() + 1; // include \0
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'p');
        buf.put_u32(total_len as u32);
        buf.extend_from_slice(self.password.as_bytes());
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.password.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn make_frame() -> PasswordMessageFrame<'static> {
        PasswordMessageFrame { password: "secret" }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = PasswordMessageFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.password, frame.password);
    }

    #[test]
    fn serialize_with_null() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"p\x00\x00\x00\x0Bsecret\x00"; // length=11 (4 + 6 + 1)
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_with_null() {
        let data = b"p\x00\x00\x00\x0Bsecret\x00";
        let frame = PasswordMessageFrame::from_bytes(data).unwrap();
        assert_eq!(frame.password, "secret");
    }

    #[test]
    fn unexpected_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'x'); // wrong tag
        buf.put_u32(4 + 7);
        buf.put_slice(b"secret\x00");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(&raw).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedTag(t) if t == b'x');
    }

    #[test]
    fn unexpected_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(10); // claims 10 (4+6), but body=5 ("short" no \0)
        buf.put_slice(b"short");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(&raw).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedLength(10));
    }

    #[test]
    fn unexpected_length_short_buffer() {
        let raw = b"p\x00\x00"; // too short
        let err = PasswordMessageFrame::from_bytes(raw).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedLength(len) if len == raw.len() as u32);
    }

    #[test]
    fn missing_null_terminator() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(4 + 6); // length for "secret" without \0
        buf.put_slice(b"secret");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(&raw).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedEof);
    }

    #[test]
    fn invalid_utf8() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(4 + 3); // invalid UTF-8 + \0
        buf.put_slice(&[0xFF, b'a', 0]);
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(&raw).unwrap_err();
        matches!(err, PasswordMessageError::Utf8Error(_));
    }

    #[test]
    fn embedded_null() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(4 + 8); // length for full body with embedded \0
        buf.put_slice(b"sec\x00ret\x00");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(&raw).unwrap_err();
        matches!(err, PasswordMessageError::ExtraDataAfterNull); // errors on extra data after first \0
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
