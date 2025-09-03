//! Module: wire_protocol::backend::copy_out_response
//!
//! Provides parsing and serialization for the CopyOutResponse message ('G') in the protocol.
//!
//! - `CopyOutResponseFrame`: represents the CopyOutResponse message with overall format and column formats.
//! - `CopyOutResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CopyOutResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::shared_property_types::ResultFormat;
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOutResponseFrame {
    pub format: ResultFormat,
    pub column_formats: Vec<ResultFormat>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CopyOutResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    InvalidFormatCode(i8),
    InvalidColumnFormatCode(i16),
}

impl fmt::Display for CopyOutResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyOutResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CopyOutResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CopyOutResponseError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            CopyOutResponseError::InvalidColumnFormatCode(c) => {
                write!(f, "invalid column format code: {c}")
            }
        }
    }
}

impl StdError for CopyOutResponseError {}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn decode_format_code(code: i16) -> Result<ResultFormat, CopyOutResponseError> {
    match code {
        0 => Ok(ResultFormat::Text),
        1 => Ok(ResultFormat::Binary),
        other => Err(CopyOutResponseError::InvalidColumnFormatCode(other)),
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

impl<'a> WireSerializable<'a> for CopyOutResponseFrame {
    type Error = CopyOutResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(CopyOutResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'G' {
            return Err(CopyOutResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            return Err(CopyOutResponseError::UnexpectedLength(len));
        }

        let format_code = bytes.get_i8();
        let format = match format_code {
            0 => ResultFormat::Text,
            1 => ResultFormat::Binary,
            other => return Err(CopyOutResponseError::InvalidFormatCode(other)),
        };

        let num_cols = bytes.get_i16() as usize;

        if bytes.remaining() != num_cols * 2 {
            return Err(CopyOutResponseError::UnexpectedLength(len));
        }

        let mut column_formats = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let col_code = bytes.get_i16();
            column_formats.push(decode_format_code(col_code)?);
        }

        Ok(CopyOutResponseFrame {
            format,
            column_formats,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_size = 1 + 2 + self.column_formats.len() * 2;
        let total_len = 4 + body_size;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'G');
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

    fn make_text_frame() -> CopyOutResponseFrame {
        CopyOutResponseFrame {
            format: ResultFormat::Text,
            column_formats: vec![ResultFormat::Text, ResultFormat::Text],
        }
    }

    fn make_binary_frame() -> CopyOutResponseFrame {
        CopyOutResponseFrame {
            format: ResultFormat::Binary,
            column_formats: vec![ResultFormat::Binary],
        }
    }

    #[test]
    fn serialize_text() {
        let frame = make_text_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"G\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_text() {
        let data = b"G\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        let frame = CopyOutResponseFrame::from_bytes(data).unwrap();
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
        let decoded = CopyOutResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.format, original.format);
        assert_eq!(decoded.column_formats, original.column_formats);
    }

    #[test]
    fn roundtrip_binary() {
        let original = make_binary_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CopyOutResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.format, original.format);
        assert_eq!(decoded.column_formats, original.column_formats);
    }

    #[test]
    fn invalid_tag() {
        let data = b"H\x00\x00\x00\x0B\x00\x00\x02\x00\x00\x00\x00";
        let err = CopyOutResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyOutResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"G\x00\x00\x00\x0C\x00\x00\x02\x00\x00\x00\x00";
        let err = CopyOutResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyOutResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_format_code() {
        let data = b"G\x00\x00\x00\x09\x02\x00\x01\x00\x00";
        let err = CopyOutResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyOutResponseError::InvalidFormatCode(2));
    }

    #[test]
    fn invalid_column_format_code() {
        let data = b"G\x00\x00\x00\x09\x00\x00\x01\x00\x02";
        let err = CopyOutResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyOutResponseError::InvalidColumnFormatCode(2));
    }

    #[test]
    fn short_column_formats() {
        let data = b"G\x00\x00\x00\x0B\x00\x00\x02\x00\x00"; // missing last i16
        let err = CopyOutResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, CopyOutResponseError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
