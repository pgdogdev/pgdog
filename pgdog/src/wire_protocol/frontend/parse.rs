//! Module: wire_protocol::frontend::parse
//!
//! Provides parsing and serialization for the Parse message ('P') in the extended protocol.
//!
//! - `ParseFrame`: represents a Parse message with statement name, query, and parameter type OIDs.
//! - `ParseFrameError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ParseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug)]
pub struct ParseFrame<'a> {
    pub statement: &'a str,
    pub query: &'a str,
    pub param_types: Vec<u32>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ParseFrameError {
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
    UnexpectedTag(u8),
}

impl fmt::Display for ParseFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseFrameError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            ParseFrameError::UnexpectedEof => write!(f, "unexpected EOF"),
            ParseFrameError::InvalidLength => write!(f, "invalid length"),
            ParseFrameError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for ParseFrameError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ParseFrameError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, ParseFrameError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(ParseFrameError::UnexpectedEof)?;
    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(ParseFrameError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ParseFrame<'a> {
    type Error = ParseFrameError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(ParseFrameError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'P' {
            return Err(ParseFrameError::UnexpectedTag(tag));
        }

        let _len = bytes.get_u32();

        let statement = read_cstr(&mut bytes)?;

        let query = read_cstr(&mut bytes)?;

        let num_params = bytes.get_i16() as usize;

        let mut param_types = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            if bytes.remaining() < 4 {
                return Err(ParseFrameError::UnexpectedEof);
            }
            param_types.push(bytes.get_u32());
        }

        if bytes.has_remaining() {
            return Err(ParseFrameError::InvalidLength);
        }

        Ok(ParseFrame {
            statement,
            query,
            param_types,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.extend_from_slice(self.statement.as_bytes());
        body.put_u8(0);

        body.extend_from_slice(self.query.as_bytes());
        body.put_u8(0);

        body.put_i16(self.param_types.len() as i16);

        for &oid in &self.param_types {
            body.put_u32(oid);
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'P');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        let mut n = self.statement.len() + 1;
        n += self.query.len() + 1;
        n += 2;
        n += self.param_types.len() * 4;
        n
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame_no_params() -> ParseFrame<'static> {
        ParseFrame {
            statement: "stmt",
            query: "SELECT 1",
            param_types: vec![],
        }
    }

    fn make_frame_with_params() -> ParseFrame<'static> {
        ParseFrame {
            statement: "",
            query: "SELECT $1::text",
            param_types: vec![25],
        }
    }

    #[test]
    fn roundtrip_no_params() {
        let frame = make_frame_no_params();
        let encoded = frame.to_bytes().unwrap();
        let decoded = ParseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.statement, frame.statement);
        assert_eq!(decoded.query, frame.query);
        assert_eq!(decoded.param_types, frame.param_types);
    }

    #[test]
    fn roundtrip_with_params() {
        let frame = make_frame_with_params();
        let encoded = frame.to_bytes().unwrap();
        let decoded = ParseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.statement, frame.statement);
        assert_eq!(decoded.query, frame.query);
        assert_eq!(decoded.param_types, frame.param_types);
    }

    #[test]
    fn invalid_tag() {
        let mut bytes = make_frame_no_params().to_bytes().unwrap().to_vec();
        bytes[0] = b'Q';
        let err = ParseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, ParseFrameError::UnexpectedTag(_));
    }

    #[test]
    fn unexpected_eof_in_params() {
        let mut body = BytesMut::new();
        body.extend_from_slice("stmt".as_bytes());
        body.put_u8(0);
        body.extend_from_slice("SELECT 1".as_bytes());
        body.put_u8(0);
        body.put_i16(1); // one param
                         // missing the u32 oid
        let mut frame = BytesMut::new();
        frame.put_u8(b'P');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);
        let err = ParseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, ParseFrameError::UnexpectedEof);
    }

    #[test]
    fn extra_data() {
        let mut bytes = make_frame_no_params().to_bytes().unwrap().to_vec();
        bytes.push(0); // extra byte
                       // but length is fixed, so adjust len to match
        let len_bytes = (bytes.len() - 1) as u32;
        bytes[1..5].copy_from_slice(&len_bytes.to_be_bytes());
        let err = ParseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, ParseFrameError::InvalidLength);
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame_no_params().to_bytes().unwrap().to_vec();
        bytes[5] = 0xFF; // corrupt first byte of statement
        let err = ParseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, ParseFrameError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
