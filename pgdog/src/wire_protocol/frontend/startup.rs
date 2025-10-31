//! Module: wire_protocol::frontend::startup
//!
//! Provides parsing and serialization for the Startup message in the PostgreSQL protocol.
//!
//! - `StartupFrame`: represents the initial Startup message with protocol version and connection parameters.
//! - `StartupError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `StartupFrame`.
//! Note: This handles the regular startup message (version typically 196608 for 3.0). Special messages like SSLRequest or CancelRequest are handled separately.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug)]
pub struct StartupFrame<'a> {
    pub version: i32,
    pub parameters: Vec<(&'a str, &'a str)>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum StartupError {
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
}

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StartupError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            StartupError::UnexpectedEof => write!(f, "unexpected EOF"),
            StartupError::InvalidLength => write!(f, "invalid length"),
        }
    }
}

impl StdError for StartupError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            StartupError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, StartupError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(StartupError::UnexpectedEof)?;
    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(StartupError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for StartupFrame<'a> {
    type Error = StartupError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let initial_remaining = bytes.remaining();
        if initial_remaining < 8 {
            return Err(StartupError::UnexpectedEof);
        }

        let msg_len = bytes.get_i32() as usize;
        if msg_len != initial_remaining {
            return Err(StartupError::InvalidLength);
        }

        let version = bytes.get_i32();

        let mut parameters = Vec::new();
        loop {
            let key = read_cstr(&mut bytes)?;
            if key.is_empty() {
                break;
            }
            let value = read_cstr(&mut bytes)?;
            parameters.push((key, value));
        }

        if bytes.has_remaining() {
            return Err(StartupError::InvalidLength);
        }

        Ok(StartupFrame {
            version,
            parameters,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.put_i32(self.version);

        for &(key, value) in &self.parameters {
            body.extend_from_slice(key.as_bytes());
            body.put_u8(0);
            body.extend_from_slice(value.as_bytes());
            body.put_u8(0);
        }

        body.put_u8(0);

        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32((body.len() + 4) as i32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self
            .parameters
            .iter()
            .map(|(k, v)| k.len() + 1 + v.len() + 1)
            .sum::<usize>()
            + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> StartupFrame<'static> {
        StartupFrame {
            version: 196608, // 3.0
            parameters: vec![("user", "postgres"), ("database", "mydb")],
        }
    }

    fn make_empty_params_frame() -> StartupFrame<'static> {
        StartupFrame {
            version: 196608,
            parameters: vec![],
        }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = StartupFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.version, frame.version);
        assert_eq!(decoded.parameters, frame.parameters);
    }

    #[test]
    fn roundtrip_empty_params() {
        let frame = make_empty_params_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = StartupFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.version, frame.version);
        assert_eq!(decoded.parameters, frame.parameters);
    }

    #[test]
    fn invalid_length_mismatch() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        // Corrupt length to be larger
        let corrupt_len = (bytes.len() + 1) as i32;
        bytes[0..4].copy_from_slice(&corrupt_len.to_be_bytes());
        let err = StartupFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, StartupError::InvalidLength);
    }

    #[test]
    fn unexpected_eof_short_buffer() {
        let raw = &[0u8; 4]; // too short
        let err = StartupFrame::from_bytes(raw).unwrap_err();
        matches!(err, StartupError::UnexpectedEof);
    }

    #[test]
    fn unexpected_eof_missing_terminator() {
        let mut body = BytesMut::new();
        body.put_i32(196608);
        body.extend_from_slice(b"user\0postgres\0database\0mydb"); // missing final \0
        let mut frame = BytesMut::new();
        frame.put_i32((body.len() + 4) as i32);
        frame.extend_from_slice(&body);
        let err = StartupFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, StartupError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_terminator() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes.push(1); // extra byte
                       // Adjust length to match new size
        let corrupt_len = bytes.len() as i32;
        bytes[0..4].copy_from_slice(&corrupt_len.to_be_bytes());
        let err = StartupFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, StartupError::InvalidLength);
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        // Corrupt a byte in "user"
        let user_pos = 8; // after length(4) + version(4)
        bytes[user_pos] = 0xFF;
        let err = StartupFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, StartupError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
