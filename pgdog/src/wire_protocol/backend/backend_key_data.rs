//! Module: wire_protocol::backend::backend_key_data
//!
//! Provides parsing and serialization for the BackendKeyData message ('K') in the protocol.
//!
//! - `BackendKeyDataFrame`: represents the BackendKeyData message with process ID and secret key.
//! - `BackendKeyDataError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `BackendKeyDataFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendKeyDataFrame {
    pub process_id: i32,
    pub secret_key: i32,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum BackendKeyDataError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
}

impl fmt::Display for BackendKeyDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendKeyDataError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            BackendKeyDataError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
        }
    }
}

impl StdError for BackendKeyDataError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for BackendKeyDataFrame {
    type Error = BackendKeyDataError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 13 {
            return Err(BackendKeyDataError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'K' {
            return Err(BackendKeyDataError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 12 {
            return Err(BackendKeyDataError::UnexpectedLength(len));
        }

        if bytes.len() != 1 + len as usize {
            return Err(BackendKeyDataError::UnexpectedLength(bytes.len() as u32));
        }

        let process_id = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        let secret_key = i32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);

        Ok(BackendKeyDataFrame {
            process_id,
            secret_key,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(13);
        buf.put_u8(b'K');
        buf.put_u32(12);
        buf.put_i32(self.process_id);
        buf.put_i32(self.secret_key);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        8
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> BackendKeyDataFrame {
        BackendKeyDataFrame {
            process_id: 1234,
            secret_key: 5678,
        }
    }

    #[test]
    fn serialize_backend_key_data() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"K\x00\x00\x00\x0C\x00\x00\x04\xD2\x00\x00\x16\x2E";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_backend_key_data() {
        let data = b"K\x00\x00\x00\x0C\x00\x00\x04\xD2\x00\x00\x16\x2E";
        let frame = BackendKeyDataFrame::from_bytes(data).unwrap();
        assert_eq!(frame.process_id, 1234);
        assert_eq!(frame.secret_key, 5678);
    }

    #[test]
    fn roundtrip_backend_key_data() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = BackendKeyDataFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = b"R\x00\x00\x00\x0C\x00\x00\x04\xD2\x00\x00\x16\x2E";
        let err = BackendKeyDataFrame::from_bytes(data).unwrap_err();
        matches!(err, BackendKeyDataError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"K\x00\x00\x00\x0D\x00\x00\x04\xD2\x00\x00\x16\x2E";
        let err = BackendKeyDataFrame::from_bytes(data).unwrap_err();
        matches!(err, BackendKeyDataError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after() {
        let data = b"K\x00\x00\x00\x0C\x00\x00\x04\xD2\x00\x00\x16\x2E\x00";
        let err = BackendKeyDataFrame::from_bytes(data).unwrap_err();
        matches!(err, BackendKeyDataError::UnexpectedLength(_));
    }

    #[test]
    fn short_data() {
        let data = b"K\x00\x00\x00\x0C\x00\x00\x04\xD2\x00\x00\x16";
        let err = BackendKeyDataFrame::from_bytes(data).unwrap_err();
        matches!(err, BackendKeyDataError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
