//! Module: wire_protocol::backend::copy_in_response
//!
//! Provides parsing and serialization for the CopyInResponse message ('G') in the protocol.
//!
//! - `CopyInResponseFrame`: represents the CopyInResponse message with overall format and per-column formats.
//! - `CopyInResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CopyInResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::shared_property_types::ResultFormat;
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyInResponseFrame {
    pub overall_format: ResultFormat,
    pub column_formats: Vec<ResultFormat>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyInResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedEof,
    InvalidOverallFormat(i8),
    InvalidColumnFormat(i16),
}

impl fmt::Display for CopyInResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyInResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CopyInResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CopyInResponseError::UnexpectedEof => write!(f, "unexpected EOF"),
            CopyInResponseError::InvalidOverallFormat(c) => {
                write!(f, "invalid overall format code: {c}")
            }
            CopyInResponseError::InvalidColumnFormat(c) => {
                write!(f, "invalid column format code: {c}")
            }
        }
    }
}

impl StdError for CopyInResponseError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CopyInResponseFrame {
    type Error = CopyInResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        // need at least tag (1) + len (4)
        if bytes.remaining() < 5 {
            return Err(CopyInResponseError::UnexpectedEof);
        }

        let tag = bytes.get_u8();
        if tag != b'G' {
            return Err(CopyInResponseError::UnexpectedTag(tag));
        }

        // need length field
        if bytes.remaining() < 4 {
            return Err(CopyInResponseError::UnexpectedEof);
        }
        let len = bytes.get_u32();
        // minimum frame length = 4 (len field) + 1 (overall) + 2 (count) = 7
        if len < 7 {
            return Err(CopyInResponseError::UnexpectedLength(len));
        }
        let payload_len = (len - 4) as usize;
        let rem = bytes.remaining();
        if rem < payload_len {
            return Err(CopyInResponseError::UnexpectedEof);
        }
        if rem > payload_len {
            return Err(CopyInResponseError::UnexpectedLength(len));
        }

        // now parse payload
        let overall_code = bytes.get_i8();
        let overall_format = match overall_code {
            0 => ResultFormat::Text,
            1 => ResultFormat::Binary,
            c => return Err(CopyInResponseError::InvalidOverallFormat(c)),
        };

        let num_i16 = bytes.get_i16();
        if num_i16 < 0 {
            return Err(CopyInResponseError::UnexpectedLength(len));
        }
        let num = num_i16 as usize;

        // expect exactly 2*num bytes left for column formats
        if bytes.remaining() < 2 * num {
            return Err(CopyInResponseError::UnexpectedEof);
        }

        let mut column_formats = Vec::with_capacity(num);
        for _ in 0..num {
            let code = bytes.get_i16();
            let fmt = match code {
                0 => ResultFormat::Text,
                1 => ResultFormat::Binary,
                c => return Err(CopyInResponseError::InvalidColumnFormat(c)),
            };
            column_formats.push(fmt);
        }

        Ok(CopyInResponseFrame {
            overall_format,
            column_formats,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        body.put_i8(if matches!(self.overall_format, ResultFormat::Text) {
            0
        } else {
            1
        });
        body.put_i16(self.column_formats.len() as i16);
        for fmt in &self.column_formats {
            body.put_i16(if matches!(fmt, ResultFormat::Text) {
                0
            } else {
                1
            });
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'G');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        1 + 2 + 2 * self.column_formats.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_text_frame() -> CopyInResponseFrame {
        CopyInResponseFrame {
            overall_format: ResultFormat::Text,
            column_formats: vec![ResultFormat::Text, ResultFormat::Text],
        }
    }

    fn make_binary_frame() -> CopyInResponseFrame {
        CopyInResponseFrame {
            overall_format: ResultFormat::Binary,
            column_formats: vec![ResultFormat::Text, ResultFormat::Binary],
        }
    }

    #[test]
    fn serialize_copy_in_response_text() {
        let frame = make_text_frame();
        let bytes = frame.to_bytes().unwrap();
        // 'G' + length(4 + 1 + 2 + 4 = 11) + overall(0) + count(2) + fmt(0,0)
        let expected = &[b'G', 0, 0, 0, 11, 0, 0, 2, 0, 0, 0, 0];
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn serialize_copy_in_response_binary() {
        let frame = make_binary_frame();
        let bytes = frame.to_bytes().unwrap();
        // 'G' + length(11) + overall(1) + count(2) + fmt(0,1)
        let expected = &[b'G', 0, 0, 0, 11, 1, 0, 2, 0, 0, 0, 1];
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_copy_in_response_text() {
        let data = &[b'G', 0, 0, 0, 11, 0, 0, 2, 0, 0, 0, 0];
        let frame = CopyInResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.overall_format, ResultFormat::Text);
        assert_eq!(
            frame.column_formats,
            vec![ResultFormat::Text, ResultFormat::Text]
        );
    }

    #[test]
    fn deserialize_copy_in_response_binary() {
        let data = &[b'G', 0, 0, 0, 11, 1, 0, 2, 0, 0, 0, 1];
        let frame = CopyInResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.overall_format, ResultFormat::Binary);
        assert_eq!(
            frame.column_formats,
            vec![ResultFormat::Text, ResultFormat::Binary]
        );
    }

    #[test]
    fn roundtrip_copy_in_response_text() {
        let original = make_text_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyInResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_copy_in_response_binary() {
        let original = make_binary_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyInResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_tag() {
        let data = vec![b'H', 0, 0, 0, 11, 0, 0, 2, 0, 0, 0, 0];
        let err = CopyInResponseFrame::from_bytes(&data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::UnexpectedTag(_)));
    }

    #[test]
    fn invalid_length_short() {
        let data = &[b'G', 0, 0, 0, 6];
        let err = CopyInResponseFrame::from_bytes(data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::UnexpectedLength(_)));
    }

    #[test]
    fn invalid_length_mismatch() {
        let data = vec![b'G', 0, 0, 0, 11, 0, 0, 2, 0, 0, 0, 0, 0]; // extra byte
        let err = CopyInResponseFrame::from_bytes(&data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::UnexpectedLength(_)));
    }

    #[test]
    fn unexpected_eof() {
        let data = &[b'G', 0, 0, 0, 11, 0, 0, 2, 0, 0];
        let err = CopyInResponseFrame::from_bytes(data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::UnexpectedEof));
    }

    #[test]
    fn invalid_overall_format() {
        let data = &[b'G', 0, 0, 0, 7, 2, 0, 0];
        let err = CopyInResponseFrame::from_bytes(data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::InvalidOverallFormat(2)));
    }

    #[test]
    fn invalid_column_format() {
        let data = &[b'G', 0, 0, 0, 9, 1, 0, 1, 0, 2];
        let err = CopyInResponseFrame::from_bytes(data).unwrap_err();
        assert!(matches!(err, CopyInResponseError::InvalidColumnFormat(2)));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
