//! Module: wire_protocol::backend::function_call_response
//!
//! Provides parsing and serialization for the FunctionCallResponse message ('V') in the protocol.
//!
//! - `FunctionCallResponseFrame`: represents the FunctionCallResponse message with the optional result value.
//! - `FunctionCallResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `FunctionCallResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCallResponseFrame<'a> {
    pub result: Option<&'a [u8]>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum FunctionCallResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    InvalidValueLength,
}

impl fmt::Display for FunctionCallResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionCallResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            FunctionCallResponseError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            FunctionCallResponseError::InvalidValueLength => write!(f, "invalid value length"),
        }
    }
}

impl StdError for FunctionCallResponseError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for FunctionCallResponseFrame<'a> {
    type Error = FunctionCallResponseError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(FunctionCallResponseError::UnexpectedLength(
                bytes.len() as u32
            ));
        }

        let tag = bytes[0];
        if tag != b'V' {
            return Err(FunctionCallResponseError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(FunctionCallResponseError::UnexpectedLength(len));
        }

        let val_len_i32 = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

        let result = if val_len_i32 == -1 {
            if bytes.len() != 9 {
                return Err(FunctionCallResponseError::UnexpectedLength(len));
            }
            None
        } else if val_len_i32 < 0 {
            return Err(FunctionCallResponseError::InvalidValueLength);
        } else {
            let val_len = val_len_i32 as usize;
            if bytes.len() != 9 + val_len {
                return Err(FunctionCallResponseError::UnexpectedLength(len));
            }
            Some(&bytes[9..9 + val_len])
        };

        Ok(FunctionCallResponseFrame { result })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let val_len_i32 = self.result.map_or(-1i32, |r| r.len() as i32);
        let value_size = if val_len_i32 >= 0 {
            val_len_i32 as usize
        } else {
            0
        };
        let contents_size = 4 + value_size; // val_len + value
        let length = 4 + contents_size; // length field + contents

        let mut buf = BytesMut::with_capacity(1 + length);
        buf.put_u8(b'V');
        buf.put_u32(length as u32);
        buf.put_i32(val_len_i32);
        if let Some(result) = self.result {
            buf.extend_from_slice(result);
        }

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self.result.map_or(0, |r| r.len())
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame_with_result<'a>() -> FunctionCallResponseFrame<'a> {
        FunctionCallResponseFrame {
            result: Some(b"result_value"),
        }
    }

    fn make_frame_null() -> FunctionCallResponseFrame<'static> {
        FunctionCallResponseFrame { result: None }
    }

    #[test]
    fn serialize_with_result() {
        let frame = make_frame_with_result();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"V\x00\x00\x00\x14\x00\x00\x00\x0Cresult_value";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_with_result() {
        let data = b"V\x00\x00\x00\x14\x00\x00\x00\x0Cresult_value";
        let frame = FunctionCallResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.result, Some(b"result_value".as_ref()));
    }

    #[test]
    fn roundtrip_with_result() {
        let original = make_frame_with_result();
        let bytes = original.to_bytes().unwrap();
        let decoded = FunctionCallResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.result, original.result);
    }

    #[test]
    fn serialize_null() {
        let frame = make_frame_null();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"V\x00\x00\x00\x08\xff\xff\xff\xff";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_null() {
        let data = b"V\x00\x00\x00\x08\xff\xff\xff\xff";
        let frame = FunctionCallResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.result, None);
    }

    #[test]
    fn roundtrip_null() {
        let original = make_frame_null();
        let bytes = original.to_bytes().unwrap();
        let decoded = FunctionCallResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.result, original.result);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\xff\xff\xff\xff";
        let err = FunctionCallResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, FunctionCallResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length_null() {
        let data = b"V\x00\x00\x00\x09\xff\xff\xff\xff";
        let err = FunctionCallResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, FunctionCallResponseError::UnexpectedLength(_));
    }

    #[test]
    fn extra_bytes_null() {
        let data = b"V\x00\x00\x00\x08\xff\xff\xff\xff\x00";
        let err = FunctionCallResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, FunctionCallResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_value_length_negative() {
        let data = b"V\x00\x00\x00\x08\xff\xff\xff\xfe";
        let err = FunctionCallResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, FunctionCallResponseError::InvalidValueLength);
    }

    #[test]
    fn short_value() {
        let data = b"V\x00\x00\x00\x14\x00\x00\x00\x0Cresult_valu"; // one byte short
        let err = FunctionCallResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, FunctionCallResponseError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
