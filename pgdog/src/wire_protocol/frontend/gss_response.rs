//! Module: wire_protocol::frontend::gss_response
//!
//! Provides parsing and serialization for the GSSResponse message ('p') used in GSSAPI authentication.
//!
//! This message is sent by the client in response to AuthenticationGSS or AuthenticationSSPI.
//!
//! - `GssResponseFrame`: represents a GSSResponse message carrying a chunk of GSS/SSPI data.
//! - `GssResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `GssResponseFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GssResponseFrame<'a> {
    pub data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum GssResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for GssResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GssResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            GssResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {}", len),
        }
    }
}

impl StdError for GssResponseError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for GssResponseFrame<'a> {
    type Error = GssResponseError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(GssResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'p' {
            return Err(GssResponseError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(GssResponseError::UnexpectedLength(len));
        }

        Ok(GssResponseFrame { data: &bytes[5..] })
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

    fn make_frame() -> GssResponseFrame<'static> {
        GssResponseFrame { data: b"gsstoken" }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = GssResponseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.data, frame.data);
    }

    #[test]
    fn unexpected_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'x'); // wrong tag, should be b'p'
        buf.put_u32(4 + 4);
        buf.put_slice(b"test");
        let raw = buf.freeze().to_vec();
        let err = GssResponseFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, GssResponseError::UnexpectedTag(t) if t == b'x');
    }

    #[test]
    fn unexpected_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_u32(10);
        buf.put_slice(b"short");
        let raw = buf.freeze().to_vec();
        let err = GssResponseFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, GssResponseError::UnexpectedLength(10));
    }

    #[test]
    fn unexpected_length_short_buffer() {
        let raw = b"p\x00\x00"; // too short to contain length + data
        let err = GssResponseFrame::from_bytes(raw).unwrap_err();
        matches!(err, GssResponseError::UnexpectedLength(len) if len == raw.len() as u32);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
