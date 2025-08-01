//! Module: wire_protocol::bidirectional::copy_data
//!
//! Provides parsing and serialization for the CopyData message ('d') in the extended protocol.
//! This message can be used by both the client and the server.
//!
//! - `CopyDataFrame`: represents a CopyData message carrying a chunk of data.
//! - `CopyDataError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CopyDataFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CopyDataFrame<'a> {
    pub data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyDataError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for CopyDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyDataError::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            CopyDataError::UnexpectedLength(len) => write!(f, "unexpected length: {}", len),
        }
    }
}

impl StdError for CopyDataError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CopyDataFrame<'a> {
    type Error = CopyDataError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(CopyDataError::UnexpectedLength(bytes.len() as u32));
        }
        let tag = bytes[0];
        if tag != b'd' {
            return Err(CopyDataError::UnexpectedTag(tag));
        }
        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(CopyDataError::UnexpectedLength(len));
        }
        Ok(CopyDataFrame { data: &bytes[5..] })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let total = 4 + self.data.len();
        let mut buf = BytesMut::with_capacity(1 + total);
        buf.put_u8(b'd');
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

    fn make_frame() -> CopyDataFrame<'static> {
        CopyDataFrame { data: b"hello" }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = CopyDataFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.data, frame.data);
    }

    #[test]
    fn unexpected_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'x'); // wrong tag, should be b'd'
        buf.put_u32(4);
        buf.put_slice(b"test");
        let raw = buf.freeze().to_vec();
        let err = CopyDataFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, CopyDataError::UnexpectedTag(t) if t == b'x');
    }

    #[test]
    fn unexpected_length_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_u32(10);
        buf.put_slice(b"short");
        let raw = buf.freeze().to_vec();
        let err = CopyDataFrame::from_bytes(raw.as_ref()).unwrap_err();
        matches!(err, CopyDataError::UnexpectedLength(10));
    }

    #[test]
    fn unexpected_length_short_buffer() {
        let raw = b"d\x00\x00"; // too short to contain length + data
        let err = CopyDataFrame::from_bytes(raw).unwrap_err();
        matches!(err, CopyDataError::UnexpectedLength(len) if len == raw.len() as u32);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
