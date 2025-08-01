//! Module: wire_protocol::frontend::execute
//!
//! Provides parsing and serialization for the Execute message ('E') in the
//! extended query protocol.
//!
//! - `ExecuteFrame`: represents a request to run a portal, containing the portal
//!   name (empty string == unnamed) and `max_rows` (0 == unlimited).
//! - Implements `WireSerializable` for lossless conversion between raw bytes and
//!   `ExecuteFrame` instances.
//!
//! Frame layout (FE → BE)
//!   0            -> 'E' (message tag)
//!   1-4          -> Int32 length: size of body + 4
//!   5...n        -> portal name (null-terminated UTF-8)
//!   n+1...n+4    -> Int32 `max_rows` (0 == unlimited)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteFrame<'a> {
    pub portal: &'a str,
    pub max_rows: u32,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ExecuteError {
    Utf8Error(str::Utf8Error),
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utf8Error(e) => write!(f, "utf-8 error: {}", e),
            Self::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            Self::UnexpectedLength(n) => write!(f, "unexpected length: {}", n),
            Self::UnexpectedEof => write!(f, "unexpected end-of-frame"),
        }
    }
}
impl StdError for ExecuteError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ExecuteFrame<'a> {
    type Error = ExecuteError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        // Need at least tag + len
        if bytes.len() < 5 {
            return Err(Self::Error::UnexpectedEof);
        }

        // Tag check
        let tag = bytes.get_u8();
        if tag != b'E' {
            return Err(Self::Error::UnexpectedTag(tag));
        }

        // Declared length (body + 4)
        let declared = bytes.get_u32();
        if declared as usize != bytes.len() {
            return Err(Self::Error::UnexpectedLength(declared));
        }

        // portal (null-terminated)
        let null_pos = bytes
            .iter()
            .position(|b| *b == 0)
            .ok_or(Self::Error::UnexpectedEof)?;

        let portal = str::from_utf8(&bytes[..null_pos]).map_err(Self::Error::Utf8Error)?;

        // Remaining after \0 must be exactly 4 bytes for max_rows
        if bytes.len() < null_pos + 1 + 4 {
            return Err(Self::Error::UnexpectedEof);
        }

        let max_rows = (&bytes[null_pos + 1..null_pos + 5]).get_u32();

        // Extra trailing data?
        if bytes.len() != null_pos + 1 + 4 {
            return Err(Self::Error::UnexpectedLength(declared));
        }

        Ok(Self { portal, max_rows })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut buf = BytesMut::with_capacity(1 + 4 + self.portal.len() + 1 + 4);
        buf.put_u8(b'E');

        let len = (self.portal.len() + 1 + 4) as u32;
        buf.put_u32(len);

        buf.put_slice(self.portal.as_bytes());
        buf.put_u8(0); // null terminator
        buf.put_u32(self.max_rows);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.portal.len() + 1 + 4
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> ExecuteFrame<'a> {
        ExecuteFrame {
            portal: "",
            max_rows: 0,
        }
    }

    fn make_named_frame<'a>() -> ExecuteFrame<'a> {
        ExecuteFrame {
            portal: "__pgdog_1",
            max_rows: 0,
        }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = ExecuteFrame::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn named_roundtrip() {
        let frame = make_named_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = ExecuteFrame::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    /// Ensure the encoder writes the correct length field for a named portal.
    ///
    /// Frame anatomy for an Execute message:
    ///   Byte 0      : 'E' tag
    ///   Bytes 1–4   : Int32 length = size of *body* + 4 (Postgres rule)
    ///   Body layout : portal_name + NUL terminator + max_rows(Int32)
    ///
    /// For the portal "__pgdog_1":
    ///   • portal bytes       = 9
    ///   • NUL terminator     = 1
    ///   • max_rows field     = 4
    ///   => body size         = 14
    ///   => length field must = 14
    ///   => total frame size  = tag(1) + len(4) + body(14) = 19 bytes
    #[test]
    fn named_len() {
        let frame = make_named_frame();
        let encoded = frame.to_bytes().unwrap();

        // Pull out the 4-byte length field (big-endian) at bytes 1–4.
        let declared_len = u32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);

        // Check the declared length as well as the overall byte count.
        assert_eq!(declared_len, 14, "length field should equal body size");
        assert_eq!(encoded.len(), 1 + 4 + 14, "total frame size mismatch");
    }

    #[test]
    fn unexpected_tag() {
        let mut bad = BytesMut::new();
        bad.put_u8(b'X'); // wrong tag, should be 'E'
        bad.put_u32(5);
        bad.put_u8(0);
        bad.put_u32(0);

        assert!(matches!(
            ExecuteFrame::from_bytes(&bad),
            Err(ExecuteError::UnexpectedTag(b'X'))
        ));
    }

    #[test]
    fn length_mismatch() {
        let mut bad = BytesMut::new();
        bad.put_u8(b'E');
        bad.put_u32(999); // bogus
        bad.put_u8(0);
        bad.put_u32(0);

        assert!(matches!(
            ExecuteFrame::from_bytes(&bad),
            Err(ExecuteError::UnexpectedLength(999))
        ));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
