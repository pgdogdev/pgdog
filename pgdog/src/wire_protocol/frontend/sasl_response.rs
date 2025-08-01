//! Module: wire_protocol::frontend::sasl_response
//!
//! Provides parsing and serialization for the SASLResponse message ('p') in the extended protocol.
//! This is used for continuation responses in SASL authentication.
//!
//! - `SaslResponseFrame`: represents a SASLResponse message carrying a chunk of response data.
//! - `SaslResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `SaslResponseFrame`.
//! Note: This is distinct from SASLInitialResponse or plain password messages, which may use the same tag but different formats.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SaslResponseFrame<'a> {
    pub data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SaslResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for SaslResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SaslResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            SaslResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {}", len),
        }
    }
}

impl StdError for SaslResponseError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for SaslResponseFrame<'a> {
    type Error = SaslResponseError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(SaslResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'p' {
            return Err(SaslResponseError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(SaslResponseError::UnexpectedLength(len));
        }

        Ok(SaslResponseFrame { data: &bytes[5..] })
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

    fn make_frame() -> SaslResponseFrame<'static> {
        SaslResponseFrame {
            data: b"bi=rO0ABXNyA",
        } // example SCRAM response
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = SaslResponseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.data, frame.data);
    }

    #[test]
    fn unexpected_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'x'); // wrong tag
        buf.put_u32(4 + 5);
        buf.put_slice(b"test");
        let raw = buf.freeze().to_vec();
        let err = SaslResponseFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, SaslResponseError::UnexpectedTag(t) if t == b'x');
    }

    #[test]
    fn unexpected_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(10);
        buf.put_slice(b"short");
        let raw = buf.freeze().to_vec();
        let err = SaslResponseFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, SaslResponseError::UnexpectedLength(10));
    }

    #[test]
    fn unexpected_length_short_buffer() {
        let raw = b"p\x00\x00"; // too short
        let err = SaslResponseFrame::from_bytes(raw).unwrap_err();
        matches!(err, SaslResponseError::UnexpectedLength(len) if len == raw.len() as u32);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
