//! Module: wire_protocol::backend::parameter_status
//!
//! Provides parsing and serialization for the ParameterStatus message ('S') in the protocol.
//!
//! - `ParameterStatusFrame`: represents the ParameterStatus message with parameter name and value.
//! - `ParameterStatusError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ParameterStatusFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterStatusFrame<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ParameterStatusError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
    Utf8Error(str::Utf8Error),
}

impl fmt::Display for ParameterStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterStatusError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            ParameterStatusError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            ParameterStatusError::UnexpectedEof => write!(f, "unexpected EOF"),
            ParameterStatusError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
        }
    }
}

impl StdError for ParameterStatusError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ParameterStatusError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(bytes: &mut &'a [u8]) -> Result<&'a str, ParameterStatusError> {
    let nul = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or(ParameterStatusError::UnexpectedEof)?;
    let (raw, rest) = bytes.split_at(nul);
    *bytes = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(ParameterStatusError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ParameterStatusFrame<'a> {
    type Error = ParameterStatusError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 7 {
            return Err(ParameterStatusError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'S' {
            return Err(ParameterStatusError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(ParameterStatusError::UnexpectedLength(len));
        }

        let mut body = &bytes[5..];
        let name = read_cstr(&mut body)?;
        let value = read_cstr(&mut body)?;

        if !body.is_empty() {
            return Err(ParameterStatusError::UnexpectedLength(len));
        }

        Ok(ParameterStatusFrame { name, value })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.extend_from_slice(self.name.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(self.value.as_bytes());
        body.put_u8(0);

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'S');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        self.name.len() + 1 + self.value.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> ParameterStatusFrame<'a> {
        ParameterStatusFrame {
            name: "client_encoding",
            value: "UTF8",
        }
    }

    fn make_empty_frame<'a>() -> ParameterStatusFrame<'a> {
        ParameterStatusFrame {
            name: "",
            value: "",
        }
    }

    #[test]
    fn serialize_parameter_status() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"S\x00\x00\x00\x15client_encoding\x00UTF8\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn serialize_empty() {
        let frame = make_empty_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"S\x00\x00\x00\x06\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_parameter_status() {
        let data = b"S\x00\x00\x00\x15client_encoding\x00UTF8\x00";
        let frame = ParameterStatusFrame::from_bytes(data).unwrap();
        assert_eq!(frame.name, "client_encoding");
        assert_eq!(frame.value, "UTF8");
    }

    #[test]
    fn deserialize_empty() {
        let data = b"S\x00\x00\x00\x06\x00\x00";
        let frame = ParameterStatusFrame::from_bytes(data).unwrap();
        assert_eq!(frame.name, "");
        assert_eq!(frame.value, "");
    }

    #[test]
    fn roundtrip_parameter_status() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = ParameterStatusFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn roundtrip_empty() {
        let original = make_empty_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = ParameterStatusFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn invalid_tag() {
        let data = b"Z\x00\x00\x00\x15client_encoding\x00UTF8\x00";
        let err = ParameterStatusFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterStatusError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"S\x00\x00\x00\x14client_encoding\x00UTF8\x00";
        let err = ParameterStatusFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterStatusError::UnexpectedLength(_));
    }

    #[test]
    fn unexpected_eof_name() {
        let data = b"S\x00\x00\x00\x15client_encodin";
        let err = ParameterStatusFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterStatusError::UnexpectedEof);
    }

    #[test]
    fn unexpected_eof_value() {
        let data = b"S\x00\x00\x00\x15client_encoding\x00UTF";
        let err = ParameterStatusFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterStatusError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after() {
        let data = b"S\x00\x00\x00\x15client_encoding\x00UTF8\x00\x00";
        let err = ParameterStatusFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterStatusError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut data = make_frame().to_bytes().unwrap().to_vec();
        data[5] = 0xFF; // corrupt name first byte
        let err = ParameterStatusFrame::from_bytes(&data).unwrap_err();
        matches!(err, ParameterStatusError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
