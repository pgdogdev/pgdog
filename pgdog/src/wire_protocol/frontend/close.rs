//! Module: wire_protocol::frontend::close
//!
//! Provides parsing and serialization for the Close message ('C') in the extended protocol.
//!
//! - `CloseFrame`: represents a Close message to close a portal or prepared statement.
//! - `CloseTarget`: enum distinguishing between portal and statement.
//! - `CloseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CloseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub struct CloseFrame<'a> {
    pub target: CloseTarget,
    pub name: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Properties :: CloseTarget ---------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget {
    Portal,
    Statement,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CloseError {
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
    InvalidTarget(u8),
    UnexpectedTag(u8),
}

impl fmt::Display for CloseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloseError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            CloseError::UnexpectedEof => write!(f, "unexpected EOF"),
            CloseError::InvalidLength => write!(f, "invalid length"),
            CloseError::InvalidTarget(t) => write!(f, "invalid target: {t:#X}"),
            CloseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for CloseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            CloseError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

#[inline]
fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, CloseError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(CloseError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(CloseError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CloseFrame<'a> {
    type Error = CloseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 6 {
            return Err(CloseError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'C' {
            return Err(CloseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len < 5 || (len - 4) as usize != bytes.remaining() {
            return Err(CloseError::InvalidLength);
        }

        let target_byte = bytes.get_u8();
        let target = match target_byte {
            b'P' => CloseTarget::Portal,
            b'S' => CloseTarget::Statement,
            _ => return Err(CloseError::InvalidTarget(target_byte)),
        };

        let name = read_cstr(&mut bytes)?;

        if !bytes.is_empty() {
            return Err(CloseError::InvalidLength);
        }

        Ok(CloseFrame { target, name })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_size = 1 + self.name.len() + 1;
        let total_len = 4 + body_size;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'C');
        buf.put_u32(total_len as u32);

        let target_byte = match self.target {
            CloseTarget::Portal => b'P',
            CloseTarget::Statement => b'S',
        };
        buf.put_u8(target_byte);
        buf.extend_from_slice(self.name.as_bytes());
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        1 + self.name.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_portal_frame() -> CloseFrame<'static> {
        CloseFrame {
            target: CloseTarget::Portal,
            name: "my_portal",
        }
    }

    fn make_statement_frame() -> CloseFrame<'static> {
        CloseFrame {
            target: CloseTarget::Statement,
            name: "my_stmt",
        }
    }

    #[test]
    fn roundtrip_portal() {
        let frame = make_portal_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = CloseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.target, frame.target);
        assert_eq!(decoded.name, frame.name);
    }

    #[test]
    fn roundtrip_statement() {
        let frame = make_statement_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = CloseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.target, frame.target);
        assert_eq!(decoded.name, frame.name);
    }

    #[test]
    fn roundtrip_empty_name() {
        let frame = CloseFrame {
            target: CloseTarget::Portal,
            name: "",
        };
        let encoded = frame.to_bytes().unwrap();
        let decoded = CloseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.target, frame.target);
        assert_eq!(decoded.name, "");
    }

    #[test]
    fn unexpected_tag() {
        let mut bytes = make_portal_frame().to_bytes().unwrap().to_vec();
        bytes[0] = b'Q'; // wrong tag
        let err = CloseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, CloseError::UnexpectedTag(t) if t == b'Q');
    }

    #[test]
    fn invalid_target() {
        let mut bytes = make_portal_frame().to_bytes().unwrap().to_vec();
        let offset = 5; // after header
        bytes[offset] = b'X'; // invalid target
        let err = CloseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, CloseError::InvalidTarget(t) if t == b'X');
    }

    #[test]
    fn invalid_length_short() {
        let bytes = b"C\x00\x00\x00\x05P"; // length 5, but body should be at least 2 (type + nul)
        let err = CloseFrame::from_bytes(bytes).unwrap_err();
        matches!(err, CloseError::UnexpectedEof);
    }

    #[test]
    fn invalid_length_mismatch() {
        let mut bytes = make_portal_frame().to_bytes().unwrap().to_vec();
        bytes[1] = 0xFF; // corrupt length
        let err = CloseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, CloseError::InvalidLength);
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let bytes = b"C\x00\x00\x00\x07Pmy"; // no nul terminator
        let err = CloseFrame::from_bytes(bytes).unwrap_err();
        matches!(err, CloseError::UnexpectedEof);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
