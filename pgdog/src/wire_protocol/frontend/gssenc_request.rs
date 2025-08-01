//! Module: wire_protocol::frontend::gssenc_request
//!
//! Provides parsing and serialization for the GSSENCRequest message in the protocol.
//!
//! Note: Unlike regular protocol messages, GSSENCRequest has no tag byte and is sent
//! by the client to request GSSAPI encryption during startup.
//!
//! - `GssencRequestFrame`: represents the GSSENCRequest message.
//! - `GssencRequestError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `GssencRequestFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GssencRequestFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum GssencRequestError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for GssencRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GssencRequestError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            GssencRequestError::UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for GssencRequestError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for GssencRequestFrame {
    type Error = GssencRequestError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err(GssencRequestError::UnexpectedLength(bytes.len()));
        }

        let mut buf = bytes;

        let len = buf.get_i32();
        if len != 8 {
            return Err(GssencRequestError::UnexpectedLength(len as usize));
        }

        let code = buf.get_i32();
        if code != 80877104 {
            return Err(GssencRequestError::UnexpectedCode(code));
        }

        Ok(GssencRequestFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i32(8);
        buf.put_i32(80877104);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        4 // code
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let frame = GssencRequestFrame;
        let encoded = frame.to_bytes().unwrap();
        let decoded = GssencRequestFrame::from_bytes(encoded.as_ref()).unwrap();
        // no state; just ensure no error
        let _ = decoded;
    }

    #[test]
    fn unexpected_length() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        // missing code
        let raw = buf.freeze().to_vec();
        let err = GssencRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, GssencRequestError::UnexpectedLength(4));
    }

    #[test]
    fn unexpected_code() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(999999);
        let raw = buf.freeze().to_vec();
        let err = GssencRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, GssencRequestError::UnexpectedCode(999999));
    }

    #[test]
    fn unexpected_length_in_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(12); // wrong length
        buf.put_i32(80877104);
        let raw = buf.freeze().to_vec();
        let err = GssencRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, GssencRequestError::UnexpectedLength(12));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
