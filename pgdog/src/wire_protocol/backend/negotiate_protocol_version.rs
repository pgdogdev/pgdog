//! Module: wire_protocol::backend::negotiate_protocol_version
//!
//! Provides parsing and serialization for the NegotiateProtocolVersion message ('v') in the protocol.
//!
//! - `NegotiateProtocolVersionFrame`: represents the NegotiateProtocolVersion message with supported minor version and unrecognized options.
//! - `NegotiateProtocolVersionError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `NegotiateProtocolVersionFrame`.

use bytes::{BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiateProtocolVersionFrame<'a> {
    pub newest_minor_version: i32,
    pub unrecognized_options: Vec<&'a str>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum NegotiateProtocolVersionError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
    Utf8Error(str::Utf8Error),
}

impl fmt::Display for NegotiateProtocolVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NegotiateProtocolVersionError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            NegotiateProtocolVersionError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            NegotiateProtocolVersionError::UnexpectedEof => write!(f, "unexpected EOF"),
            NegotiateProtocolVersionError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
        }
    }
}

impl StdError for NegotiateProtocolVersionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            NegotiateProtocolVersionError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, NegotiateProtocolVersionError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(NegotiateProtocolVersionError::UnexpectedEof)?;
    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(NegotiateProtocolVersionError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for NegotiateProtocolVersionFrame<'a> {
    type Error = NegotiateProtocolVersionError;

    fn from_bytes(bytes_full: &'a [u8]) -> Result<Self, Self::Error> {
        // need at least tag+len
        if bytes_full.len() < 5 {
            return Err(NegotiateProtocolVersionError::UnexpectedEof);
        }
        if bytes_full[0] != b'v' {
            return Err(NegotiateProtocolVersionError::UnexpectedTag(bytes_full[0]));
        }

        let len = u32::from_be_bytes([bytes_full[1], bytes_full[2], bytes_full[3], bytes_full[4]]);
        // body must contain at least 8 bytes (minor version + count)
        if len < 8 {
            return Err(NegotiateProtocolVersionError::UnexpectedLength(len));
        }

        let total_len = (len as usize) + 1; // tag + length bytes
        if bytes_full.len() < total_len {
            return Err(NegotiateProtocolVersionError::UnexpectedEof);
        }
        if bytes_full.len() > total_len {
            return Err(NegotiateProtocolVersionError::UnexpectedLength(len));
        }

        let mut payload = &bytes_full[5..total_len];
        // read minor version + num_options
        if payload.len() < 8 {
            return Err(NegotiateProtocolVersionError::UnexpectedEof);
        }
        let newest_minor_version = i32::from_be_bytes(payload[0..4].try_into().unwrap());
        let num_options = i32::from_be_bytes(payload[4..8].try_into().unwrap());
        if num_options < 0 {
            return Err(NegotiateProtocolVersionError::UnexpectedLength(len));
        }

        payload = &payload[8..];
        let mut unrecognized_options = Vec::with_capacity(num_options as usize);
        for _ in 0..num_options {
            let opt = read_cstr(&mut payload)?;
            unrecognized_options.push(opt);
        }

        if !payload.is_empty() {
            return Err(NegotiateProtocolVersionError::UnexpectedLength(len));
        }

        Ok(NegotiateProtocolVersionFrame {
            newest_minor_version,
            unrecognized_options,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.put_i32(self.newest_minor_version);
        body.put_i32(self.unrecognized_options.len() as i32);
        for opt in &self.unrecognized_options {
            body.extend_from_slice(opt.as_bytes());
            body.put_u8(0);
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'v');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        4 + 4
            + self
                .unrecognized_options
                .iter()
                .map(|opt| opt.len() + 1)
                .sum::<usize>()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame_empty<'a>() -> NegotiateProtocolVersionFrame<'a> {
        NegotiateProtocolVersionFrame {
            newest_minor_version: 0,
            unrecognized_options: vec![],
        }
    }

    fn make_frame_with_options<'a>() -> NegotiateProtocolVersionFrame<'a> {
        NegotiateProtocolVersionFrame {
            newest_minor_version: 123,
            unrecognized_options: vec!["opt1", "opt2"],
        }
    }

    #[test]
    fn serialize_empty() {
        let frame = make_frame_empty();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"v\x00\x00\x00\x0C\x00\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn serialize_with_options() {
        let frame = make_frame_with_options();
        let bytes = frame.to_bytes().unwrap();
        // len = 4 + 4+4 + (4+1)+(4+1) = 4+8+10=22
        let expected = b"v\x00\x00\x00\x16\x00\x00\x00\x7B\x00\x00\x00\x02opt1\x00opt2\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_empty() {
        let data = b"v\x00\x00\x00\x0C\x00\x00\x00\x00\x00\x00\x00\x00";
        let frame = NegotiateProtocolVersionFrame::from_bytes(data).unwrap();
        assert_eq!(frame.newest_minor_version, 0);
        assert_eq!(frame.unrecognized_options.len(), 0);
    }

    #[test]
    fn deserialize_with_options() {
        let data = b"v\x00\x00\x00\x16\x00\x00\x00\x7B\x00\x00\x00\x02opt1\x00opt2\x00";
        let frame = NegotiateProtocolVersionFrame::from_bytes(data).unwrap();
        assert_eq!(frame.newest_minor_version, 123);
        assert_eq!(frame.unrecognized_options, vec!["opt1", "opt2"]);
    }

    #[test]
    fn roundtrip_empty() {
        let original = make_frame_empty();
        let bytes = original.to_bytes().unwrap();
        let decoded = NegotiateProtocolVersionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn roundtrip_with_options() {
        let original = make_frame_with_options();
        let bytes = original.to_bytes().unwrap();
        let decoded = NegotiateProtocolVersionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn invalid_tag() {
        let data = b"V\x00\x00\x00\x0C\x00\x00\x00\x00\x00\x00\x00\x00";
        let err = NegotiateProtocolVersionFrame::from_bytes(data).unwrap_err();
        matches!(err, NegotiateProtocolVersionError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length_short() {
        let data = b"v\x00\x00\x00\x0B\x00\x00\x00\x00\x00\x00\x00\x00";
        let err = NegotiateProtocolVersionFrame::from_bytes(data).unwrap_err();
        matches!(err, NegotiateProtocolVersionError::UnexpectedLength(_));
    }

    #[test]
    fn unexpected_eof_option() {
        let data = b"v\x00\x00\x00\x11\x00\x00\x00\x00\x00\x00\x00\x01opt"; // no null
        let err = NegotiateProtocolVersionFrame::from_bytes(data).unwrap_err();
        matches!(err, NegotiateProtocolVersionError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after() {
        let data = b"v\x00\x00\x00\x0C\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let err = NegotiateProtocolVersionFrame::from_bytes(data).unwrap_err();
        matches!(err, NegotiateProtocolVersionError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let data = vec![b'v', 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 1, 0xFF, 0]; // invalid UTF8
        let err = NegotiateProtocolVersionFrame::from_bytes(&data).unwrap_err();
        matches!(err, NegotiateProtocolVersionError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
