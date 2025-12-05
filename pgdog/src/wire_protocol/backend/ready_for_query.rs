//! Module: wire_protocol::backend::ready_for_query
//!
//! Provides parsing and serialization for the ReadyForQuery message ('Z') in the protocol.
//!
//! - `ReadyForQueryFrame`: represents the ReadyForQuery message with transaction status.
//! - `ReadyForQueryError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ReadyForQueryFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyForQueryFrame {
    pub status: TransactionStatus,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    InFailedTransaction,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ReadyForQueryError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    InvalidStatus(u8),
}

impl fmt::Display for ReadyForQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadyForQueryError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            ReadyForQueryError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            ReadyForQueryError::InvalidStatus(s) => write!(f, "invalid status: {s:#X}"),
        }
    }
}

impl StdError for ReadyForQueryError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ReadyForQueryFrame {
    type Error = ReadyForQueryError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 6 {
            return Err(ReadyForQueryError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'Z' {
            return Err(ReadyForQueryError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len != 5 {
            return Err(ReadyForQueryError::UnexpectedLength(len));
        }

        let status_byte = bytes[5];
        let status = match status_byte {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InTransaction,
            b'E' => TransactionStatus::InFailedTransaction,
            other => return Err(ReadyForQueryError::InvalidStatus(other)),
        };

        Ok(ReadyForQueryFrame { status })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let status_byte = match self.status {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::InFailedTransaction => b'E',
        };
        let mut buf = BytesMut::with_capacity(6);
        buf.put_u8(b'Z');
        buf.put_u32(5);
        buf.put_u8(status_byte);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_idle() {
        let frame = ReadyForQueryFrame {
            status: TransactionStatus::Idle,
        };
        let bytes = frame.to_bytes().unwrap();
        let expected = b"Z\x00\x00\x00\x05I";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_idle() {
        let data = b"Z\x00\x00\x00\x05I";
        let frame = ReadyForQueryFrame::from_bytes(data).unwrap();
        assert_eq!(frame.status, TransactionStatus::Idle);
    }

    #[test]
    fn roundtrip_idle() {
        let original = ReadyForQueryFrame {
            status: TransactionStatus::Idle,
        };
        let bytes = original.to_bytes().unwrap();
        let decoded = ReadyForQueryFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.status, original.status);
    }

    #[test]
    fn roundtrip_in_transaction() {
        let original = ReadyForQueryFrame {
            status: TransactionStatus::InTransaction,
        };
        let bytes = original.to_bytes().unwrap();
        let decoded = ReadyForQueryFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.status, original.status);
    }

    #[test]
    fn roundtrip_in_failed_transaction() {
        let original = ReadyForQueryFrame {
            status: TransactionStatus::InFailedTransaction,
        };
        let bytes = original.to_bytes().unwrap();
        let decoded = ReadyForQueryFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.status, original.status);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x05I";
        let err = ReadyForQueryFrame::from_bytes(data).unwrap_err();
        matches!(err, ReadyForQueryError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"Z\x00\x00\x00\x06I";
        let err = ReadyForQueryFrame::from_bytes(data).unwrap_err();
        matches!(err, ReadyForQueryError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_status() {
        let data = b"Z\x00\x00\x00\x05X";
        let err = ReadyForQueryFrame::from_bytes(data).unwrap_err();
        matches!(err, ReadyForQueryError::InvalidStatus(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
