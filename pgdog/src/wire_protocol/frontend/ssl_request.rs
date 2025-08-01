//! Module: wire_protocol::frontend::ssl_request
//!
//! Provides parsing and serialization for the SSLRequest message in the protocol.
//!
//! Note: Unlike regular protocol messages, SSLRequest has no tag byte and is sent
//! by the client to request an SSL/TLS connection during startup.
//!
//! - `SslRequestFrame`: represents the SSLRequest message.
//! - `SslRequestError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `SslRequestFrame`.

use crate::wire_protocol::WireSerializable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SslRequestFrame;

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SslRequestError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for SslRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SslRequestError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            SslRequestError::UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for SslRequestError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for SslRequestFrame {
    type Error = SslRequestError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err(SslRequestError::UnexpectedLength(bytes.len()));
        }

        let mut buf = bytes;

        let len = buf.get_i32();
        if len != 8 {
            return Err(SslRequestError::UnexpectedLength(len as usize));
        }

        let code = buf.get_i32();
        if code != 80877103 {
            return Err(SslRequestError::UnexpectedCode(code));
        }

        Ok(SslRequestFrame)
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i32(8);
        buf.put_i32(80877103);
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
        let frame = SslRequestFrame;
        let encoded = frame.to_bytes().unwrap();
        let decoded = SslRequestFrame::from_bytes(encoded.as_ref()).unwrap();
        // no state; just ensure no error
        let _ = decoded;
    }

    #[test]
    fn unexpected_length() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        // missing code
        let raw = buf.freeze().to_vec();
        let err = SslRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, SslRequestError::UnexpectedLength(4));
    }

    #[test]
    fn unexpected_code() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(999999);
        let raw = buf.freeze().to_vec();
        let err = SslRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, SslRequestError::UnexpectedCode(999999));
    }

    #[test]
    fn unexpected_length_in_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(12); // wrong length
        buf.put_i32(80877103);
        let raw = buf.freeze().to_vec();
        let err = SslRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, SslRequestError::UnexpectedLength(12));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
