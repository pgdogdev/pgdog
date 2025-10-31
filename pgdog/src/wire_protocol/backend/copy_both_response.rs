//! Module: wire_protocol::backend::copy_both_response
//!
//! Provides parsing and serialization for the CopyBothResponse message ('W') in the protocol.
//!
//! - `CopyBothResponseFrame`: represents the CopyBothResponse message with overall format and column formats.
//! - `CopyBothResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CopyBothResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::shared_property_types::ResultFormat;
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBothResponseFrame {
    pub format: ResultFormat,
    pub column_formats: Vec<ResultFormat>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyBothResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    InvalidFormatCode(i8),
    InvalidColumnFormatCode(i16),
}

impl fmt::Display for CopyBothResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyBothResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CopyBothResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CopyBothResponseError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            CopyBothResponseError::InvalidColumnFormatCode(c) => {
                write!(f, "invalid column format code: {c}")
            }
        }
    }
}

impl StdError for CopyBothResponseError {}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn decode_format_code(code: i16) -> Result<ResultFormat, CopyBothResponseError> {
    match code {
        0 => Ok(ResultFormat::Text),
        1 => Ok(ResultFormat::Binary),
        other => Err(CopyBothResponseError::InvalidColumnFormatCode(other)),
    }
}

fn encode_format_code(buf: &mut BytesMut, format: ResultFormat) {
    buf.put_i16(match format {
        ResultFormat::Text => 0,
        ResultFormat::Binary => 1,
    });
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CopyBothResponseFrame {
    type Error = CopyBothResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(CopyBothResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'W' {
            return Err(CopyBothResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            return Err(CopyBothResponseError::UnexpectedLength(len));
        }

        let format_code = bytes.get_i8();
        let format = match format_code {
            0 => ResultFormat::Text,
            1 => ResultFormat::Binary,
            other => return Err(CopyBothResponseError::InvalidFormatCode(other)),
        };

        let num_cols = bytes.get_i16() as usize;

        if bytes.remaining() != num_cols * 2 {
            return Err(CopyBothResponseError::UnexpectedLength(len));
        }

        let mut column_formats = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let col_code = bytes.get_i16();
            column_formats.push(decode_format_code(col_code)?);
        }

        Ok(CopyBothResponseFrame {
            format,
            column_formats,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_size = 1 + 2 + self.column_formats.len() * 2;
        let total_len = 4 + body_size;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'W');
        buf.put_u32(total_len as u32);
        buf.put_i8(match self.format {
            ResultFormat::Text => 0,
            ResultFormat::Binary => 1,
        });
        buf.put_i16(self.column_formats.len() as i16);
        for fmt in &self.column_formats {
            encode_format_code(&mut buf, *fmt);
        }

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        1 + 2 + self.column_formats.len() * 2
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_text_frame() -> CopyBothResponseFrame {
        CopyBothResponseFrame {
            format: ResultFormat::Text,
            column_formats: vec![ResultFormat::Text, ResultFormat::Text],
        }
    }

    fn make_binary_frame() -> CopyBothResponseFrame {
        CopyBothResponseFrame {
            format: ResultFormat::Binary,
            column_formats: vec![ResultFormat::Binary],
        }
    }

    #[test]
    fn serialize_text() {
        let frame = make_text_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"W\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_text() {
        let data = b"W\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        let frame = CopyBothResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.format, ResultFormat::Text);
        assert_eq!(
            frame.column_formats,
            vec![ResultFormat::Text, ResultFormat::Text]
        );
    }

    #[test]
    fn roundtrip_text() {
        let original = make_text_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyBothResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.format, original.format);
        assert_eq!(decoded.column_formats, original.column_formats);
    }

    #[test]
    fn roundtrip_binary() {
        let original = make_binary_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyBothResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.format, original.format);
        assert_eq!(decoded.column_formats, original.column_formats);
    }

    #[test]
    fn invalid_tag() {
        let data = b"H\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        let err = CopyBothResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyBothResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"W\x00\x00\x00\x0C\x00\x00\x02\x00\x00\x00\x00";
        let err = CopyBothResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyBothResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_format_code() {
        let data = b"W\x00\x00\x00\x09\x02\x00\x01\x00\x00";
        let err = CopyBothResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyBothResponseError::InvalidFormatCode(2));
    }

    #[test]
    fn invalid_column_format_code() {
        let data = b"W\x00\x00\x00\x09\x00\x00\x01\x00\x02";
        let err = CopyBothResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyBothResponseError::InvalidColumnFormatCode(2));
    }

    #[test]
    fn short_column_formats() {
        let data = b"W\x00\x00\x00\x0B\x00\x00\x02\x00\x00"; // missing last i16
        let err = CopyBothResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyBothResponseError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
