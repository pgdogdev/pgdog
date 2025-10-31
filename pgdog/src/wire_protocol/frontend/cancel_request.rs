//! Module: wire_protocol::frontend::cancel_request
//!
//! Provides parsing and serialization for the CancelRequest message in the protocol.
//!
//! Note: Unlike regular protocol messages, CancelRequest has no tag byte and is typically
//! sent over a separate connection to interrupt a running query.
//!
//! - `CancelRequestFrame`: represents a CancelRequest message with backend PID and secret key.
//! - `CancelRequestError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CancelRequestFrame`.

use crate::wire_protocol::WireSerializable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelRequestFrame {
    pub pid: i32,
    pub secret: i32,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CancelRequestError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for CancelRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CancelRequestError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CancelRequestError::UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for CancelRequestError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CancelRequestFrame {
    type Error = CancelRequestError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 16 {
            return Err(CancelRequestError::UnexpectedLength(bytes.len()));
        }

        let mut buf = bytes;

        let len = buf.get_i32();
        if len != 16 {
            return Err(CancelRequestError::UnexpectedLength(len as usize));
        }

        let code = buf.get_i32();
        if code != 80877102 {
            return Err(CancelRequestError::UnexpectedCode(code));
        }

        let pid = buf.get_i32();
        let secret = buf.get_i32();

        Ok(CancelRequestFrame { pid, secret })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_i32(16);
        buf.put_i32(80877102);
        buf.put_i32(self.pid);
        buf.put_i32(self.secret);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        12 // code + pid + secret
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> CancelRequestFrame {
        CancelRequestFrame {
            pid: 1234,
            secret: 5678,
        }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = CancelRequestFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.pid, frame.pid);
        assert_eq!(decoded.secret, frame.secret);
    }

    #[test]
    fn unexpected_length() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(80877102);
        buf.put_i32(1234);
        // missing secret
        let raw = buf.freeze().to_vec();
        let err = CancelRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedLength(12));
    }

    #[test]
    fn unexpected_code() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(999999);
        buf.put_i32(1234);
        buf.put_i32(5678);
        let raw = buf.freeze().to_vec();
        let err = CancelRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedCode(999999));
    }

    #[test]
    fn unexpected_length_in_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(20); // wrong length
        buf.put_i32(80877102);
        buf.put_i32(1234);
        buf.put_i32(5678);
        let raw = buf.freeze().to_vec();
        let err = CancelRequestFrame::from_bytes(&raw).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedLength(20));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
