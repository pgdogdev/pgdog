//! Module: wire_protocol::frontend::password_message
//!
//! Provides parsing and serialization for the PasswordMessage message ('p') used in password authentication.
//!
//! This message is sent by the client in response to AuthenticationCleartextPassword, AuthenticationMD5Password, etc.
//!
//! - `PasswordMessageFrame`: represents a PasswordMessage message carrying the password data.
//! - `PasswordMessageError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `PasswordMessageFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PasswordMessageFrame<'a> {
    pub data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum PasswordMessageError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for PasswordMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PasswordMessageError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            PasswordMessageError::UnexpectedLength(len) => write!(f, "unexpected length: {}", len),
        }
    }
}

impl StdError for PasswordMessageError {}

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

        Ok(PasswordMessageFrame { data: &bytes[5..] })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let total = 4 + self.data.len();
        let mut buf = BytesMut::with_capacity(1 + total);
        buf.put_u8(b'p');
        buf.put_u32(total as u32);
        buf.put_slice(self.data);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.data.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn make_frame() -> PasswordMessageFrame<'static> {
        PasswordMessageFrame { data: b"secret" }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = PasswordMessageFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.data, frame.data);
    }

    #[test]
    fn unexpected_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'x'); // wrong tag, should be b'p'
        buf.put_u32(4 + 4);
        buf.put_slice(b"test");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedTag(t) if t == b'x');
    }

    #[test]
    fn unexpected_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(10);
        buf.put_slice(b"short");
        let raw = buf.freeze().to_vec();
        let err = PasswordMessageFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedLength(10));
    }

    #[test]
    fn unexpected_length_short_buffer() {
        let raw = b"p\x00\x00"; // too short to contain length + data
        let err = PasswordMessageFrame::from_bytes(raw).unwrap_err();
        matches!(err, PasswordMessageError::UnexpectedLength(len) if len == raw.len() as u32);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
