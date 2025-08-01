//! Module: wire_protocol::backend::parameter_description
//!
//! Provides parsing and serialization for the ParameterDescription message ('t') in the protocol.
//!
//! - `ParameterDescriptionFrame`: represents the ParameterDescription message with parameter type OIDs.
//! - `ParameterDescriptionError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ParameterDescriptionFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterDescriptionFrame {
    pub parameter_oids: Vec<u32>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ParameterDescriptionError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
}

impl fmt::Display for ParameterDescriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterDescriptionError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            ParameterDescriptionError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            ParameterDescriptionError::UnexpectedEof => write!(f, "unexpected EOF"),
        }
    }
}

impl StdError for ParameterDescriptionError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ParameterDescriptionFrame {
    type Error = ParameterDescriptionError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 7 {
            return Err(ParameterDescriptionError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b't' {
            return Err(ParameterDescriptionError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len < 6 {
            return Err(ParameterDescriptionError::UnexpectedLength(len));
        }
        if bytes.remaining() != (len - 4) as usize {
            return Err(ParameterDescriptionError::UnexpectedLength(len));
        }

        let num_params = bytes.get_i16();
        if num_params < 0 {
            return Err(ParameterDescriptionError::UnexpectedLength(len));
        }
        let num = num_params as usize;

        if bytes.remaining() != 4 * num {
            return Err(ParameterDescriptionError::UnexpectedEof);
        }

        let mut parameter_oids = Vec::with_capacity(num);
        for _ in 0..num {
            parameter_oids.push(bytes.get_u32());
        }

        Ok(ParameterDescriptionFrame { parameter_oids })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.put_i16(self.parameter_oids.len() as i16);
        for &oid in &self.parameter_oids {
            body.put_u32(oid);
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b't');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        2 + 4 * self.parameter_oids.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> ParameterDescriptionFrame {
        ParameterDescriptionFrame {
            parameter_oids: vec![23, 25, 1043], // int4, text, varchar
        }
    }

    fn make_empty_frame() -> ParameterDescriptionFrame {
        ParameterDescriptionFrame {
            parameter_oids: vec![],
        }
    }

    #[test]
    fn serialize_parameter_description() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"t\x00\x00\x00\x0E\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19\x00\x00\x04\x13";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn serialize_empty() {
        let frame = make_empty_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"t\x00\x00\x00\x06\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_parameter_description() {
        let data = b"t\x00\x00\x00\x0E\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19\x00\x00\x04\x13";
        let frame = ParameterDescriptionFrame::from_bytes(data).unwrap();
        assert_eq!(frame.parameter_oids, vec![23, 25, 1043]);
    }

    #[test]
    fn deserialize_empty() {
        let data = b"t\x00\x00\x00\x06\x00\x00";
        let frame = ParameterDescriptionFrame::from_bytes(data).unwrap();
        assert_eq!(frame.parameter_oids.len(), 0);
    }

    #[test]
    fn roundtrip_parameter_description() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = ParameterDescriptionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn roundtrip_empty() {
        let original = make_empty_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = ParameterDescriptionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn invalid_tag() {
        let data = b"T\x00\x00\x00\x0E\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19\x00\x00\x04\x13";
        let err = ParameterDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterDescriptionError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"t\x00\x00\x00\x0D\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19\x00\x00\x04\x13";
        let err = ParameterDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterDescriptionError::UnexpectedLength(_));
    }

    #[test]
    fn unexpected_eof() {
        let data = b"t\x00\x00\x00\x0E\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19"; // missing last OID
        let err = ParameterDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterDescriptionError::UnexpectedEof);
    }

    #[test]
    fn extra_data() {
        let data = b"t\x00\x00\x00\x0E\x00\x03\x00\x00\x00\x17\x00\x00\x00\x19\x00\x00\x04\x13\x00";
        let err = ParameterDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, ParameterDescriptionError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
