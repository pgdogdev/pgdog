//! Module: wire_protocol::frontend::copy_fail
//!
//! Provides parsing and serialization for the CopyFail message ('f') in the extended protocol.
//!
//! - `CopyFailFrame`: represents a CopyFail message sent by the client to indicate failure during COPY, with an error message.
//!
//! Implements `WireSerializable` for conversion between raw bytes and `CopyFailFrame`.

use bytes::{BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CopyFailFrame<'a> {
    pub message: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyFailError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
}

impl fmt::Display for CopyFailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyFailError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CopyFailError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CopyFailError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            CopyFailError::UnexpectedEof => write!(f, "unexpected EOF"),
        }
    }
}

impl StdError for CopyFailError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            CopyFailError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(bytes: &'a [u8]) -> Result<(&'a str, usize), CopyFailError> {
    let nul = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or(CopyFailError::UnexpectedEof)?;
    let raw = &bytes[..nul];
    let s = str::from_utf8(raw).map_err(CopyFailError::Utf8Error)?;
    Ok((s, nul + 1))
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CopyFailFrame<'a> {
    type Error = CopyFailError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(CopyFailError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'f' {
            return Err(CopyFailError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(CopyFailError::UnexpectedLength(len));
        }

        let (message, consumed) = read_cstr(&bytes[5..])?;
        if consumed != bytes.len() - 5 {
            return Err(CopyFailError::UnexpectedLength(len));
        }

        Ok(CopyFailFrame { message })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = self.message.len() + 1;
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'f');
        buf.put_u32(total_len as u32);
        buf.extend_from_slice(self.message.as_bytes());
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.message.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> CopyFailFrame<'static> {
        CopyFailFrame {
            message: "failure reason",
        }
    }

    #[test]
    fn serialize_copy_fail() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"f\x00\x00\x00\x13failure reason\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_copy_fail() {
        let data = b"f\x00\x00\x00\x13failure reason\x00";
        let frame = CopyFailFrame::from_bytes(data).unwrap();
        assert_eq!(frame.message, "failure reason");
    }

    #[test]
    fn roundtrip_copy_fail() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyFailFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.message, original.message);
    }

    #[test]
    fn invalid_tag() {
        let data = b"Q\x00\x00\x00\x13failure reason\x00";
        let err = CopyFailFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyFailError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"f\x00\x00\x00\x14failure reason\x00";
        let err = CopyFailFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyFailError::UnexpectedLength(_));
    }

    #[test]
    fn missing_null_terminator() {
        let data = b"f\x00\x00\x00\x13failure reason";
        let err = CopyFailFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyFailError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_null() {
        let data = b"f\x00\x00\x00\x13failure reason\x00extra";
        let err = CopyFailFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyFailError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[10] = 0xFF; // corrupt a byte to invalid UTF-8
        let err = CopyFailFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, CopyFailError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
