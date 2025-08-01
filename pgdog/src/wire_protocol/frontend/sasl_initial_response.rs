//! Module: wire_protocol::frontend::sasl_initial_response
//!
//! Provides parsing and serialization for the SASLInitialResponse message ('p') in the extended protocol.
//!
//! - `SaslInitialResponseFrame`: represents the initial SASL response with mechanism name and optional initial data.
//! - `SaslInitialResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `SaslInitialResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::shared_property_types::sasl_mechanism::{
    SaslMechanism, SaslMechanismError,
};
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SaslInitialResponseFrame<'a> {
    pub mechanism: SaslMechanism,
    pub initial_data: Option<&'a [u8]>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum SaslInitialResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidDataLength,
    MechanismError(SaslMechanismError),
}

impl fmt::Display for SaslInitialResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SaslInitialResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            SaslInitialResponseError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            SaslInitialResponseError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            SaslInitialResponseError::UnexpectedEof => write!(f, "unexpected EOF"),
            SaslInitialResponseError::InvalidDataLength => write!(f, "invalid data length"),
            SaslInitialResponseError::MechanismError(e) => write!(f, "SASL mechanism error: {e}"),
        }
    }
}

impl StdError for SaslInitialResponseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SaslInitialResponseError::Utf8Error(e) => Some(e),
            SaslInitialResponseError::MechanismError(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, SaslInitialResponseError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(SaslInitialResponseError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(SaslInitialResponseError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for SaslInitialResponseFrame<'a> {
    type Error = SaslInitialResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(SaslInitialResponseError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'p' {
            return Err(SaslInitialResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            // len includes itself
            return Err(SaslInitialResponseError::UnexpectedLength(len));
        }

        let mechanism_str = read_cstr(&mut bytes)?;

        let mechanism = SaslMechanism::from_str(mechanism_str)
            .map_err(SaslInitialResponseError::MechanismError)?;

        if bytes.remaining() < 4 {
            return Err(SaslInitialResponseError::UnexpectedEof);
        }

        let data_len_i32 = bytes.get_i32();

        let initial_data = if data_len_i32 == -1 {
            if bytes.has_remaining() {
                return Err(SaslInitialResponseError::UnexpectedLength(len));
            }
            None
        } else {
            if data_len_i32 < 0 {
                return Err(SaslInitialResponseError::InvalidDataLength);
            }
            let data_len = data_len_i32 as usize;
            if bytes.remaining() != data_len {
                return Err(SaslInitialResponseError::UnexpectedLength(len));
            }
            let data = &bytes[0..data_len];
            bytes.advance(data_len);
            Some(data)
        };

        Ok(SaslInitialResponseFrame {
            mechanism,
            initial_data,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::new();
        body.extend_from_slice(self.mechanism.as_str().as_bytes());
        body.put_u8(0);
        match &self.initial_data {
            Some(data) => {
                body.put_i32(data.len() as i32);
                body.extend_from_slice(data);
            }
            None => {
                body.put_i32(-1);
            }
        }

        let mut frame = BytesMut::with_capacity(5 + body.len());
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        self.mechanism.as_str().len() + 1 + 4 + self.initial_data.map_or(0, |d| d.len())
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame_with_data() -> SaslInitialResponseFrame<'static> {
        SaslInitialResponseFrame {
            mechanism: SaslMechanism::ScramSha256,
            initial_data: Some(b"n,,n=user,r=3D3D3D"),
        }
    }

    fn make_frame_no_data() -> SaslInitialResponseFrame<'static> {
        SaslInitialResponseFrame {
            mechanism: SaslMechanism::ScramSha256Plus,
            initial_data: None,
        }
    }

    #[test]
    fn roundtrip_with_data() {
        let frame = make_frame_with_data();
        let encoded = frame.to_bytes().unwrap();
        let decoded = SaslInitialResponseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.mechanism, frame.mechanism);
        assert_eq!(decoded.initial_data, frame.initial_data);
    }

    #[test]
    fn roundtrip_no_data() {
        let frame = make_frame_no_data();
        let encoded = frame.to_bytes().unwrap();
        let decoded = SaslInitialResponseFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(decoded.mechanism, frame.mechanism);
        assert_eq!(decoded.initial_data, frame.initial_data);
    }

    #[test]
    fn invalid_tag() {
        let mut bytes = make_frame_with_data().to_bytes().unwrap().to_vec();
        bytes[0] = b'Q';
        let err = SaslInitialResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, SaslInitialResponseError::UnexpectedTag(_));
    }

    #[test]
    fn unexpected_eof_missing_data_len() {
        let mut body = BytesMut::new();
        body.extend_from_slice("SCRAM-SHA-256".as_bytes());
        body.put_u8(0);
        // missing i32
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let err = SaslInitialResponseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, SaslInitialResponseError::UnexpectedEof);
    }

    #[test]
    fn invalid_data_length_negative_not_minus_one() {
        let mut body = BytesMut::new();
        body.extend_from_slice("SCRAM-SHA-256".as_bytes());
        body.put_u8(0);
        body.put_i32(-2); // invalid
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let err = SaslInitialResponseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, SaslInitialResponseError::InvalidDataLength);
    }

    #[test]
    fn unexpected_length_data_mismatch() {
        let mut body = BytesMut::new();
        body.extend_from_slice("SCRAM-SHA-256".as_bytes());
        body.put_u8(0);
        body.put_i32(10); // claims 10 bytes
        body.extend_from_slice(b"short"); // only 5
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let err = SaslInitialResponseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, SaslInitialResponseError::UnexpectedLength(_));
    }

    #[test]
    fn extra_data_after_no_data() {
        let mut body = BytesMut::new();
        body.extend_from_slice("SCRAM-SHA-256-PLUS".as_bytes());
        body.put_u8(0);
        body.put_i32(-1);
        body.put_u8(1); // extra
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let err = SaslInitialResponseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, SaslInitialResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8_mechanism() {
        let mut bytes = make_frame_with_data().to_bytes().unwrap().to_vec();
        bytes[5] = 0xFF; // corrupt mechanism byte
        let err = SaslInitialResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, SaslInitialResponseError::Utf8Error(_));
    }

    #[test]
    fn invalid_mechanism() {
        let mut body = BytesMut::new();
        body.extend_from_slice("PLAIN".as_bytes());
        body.put_u8(0);
        body.put_i32(-1);
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let err = SaslInitialResponseFrame::from_bytes(frame.as_ref()).unwrap_err();
        matches!(err, SaslInitialResponseError::MechanismError(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
