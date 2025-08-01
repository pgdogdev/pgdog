//! Module: wire_protocol::backend::row_description
//!
//! Provides parsing and serialization for the RowDescription message ('T') in the protocol.
//!
//! - `RowDescriptionFrame`: represents the RowDescription message with a list of field descriptions.
//! - `RowDescriptionError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `RowDescriptionFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::shared_property_types::ResultFormat;
use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct RowDescriptionFrame<'a> {
    pub fields: Vec<RowField<'a>>,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct RowField<'a> {
    pub name: &'a str,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: ResultFormat,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum RowDescriptionError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidFormatCode(i16),
}

impl fmt::Display for RowDescriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowDescriptionError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            RowDescriptionError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            RowDescriptionError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            RowDescriptionError::UnexpectedEof => write!(f, "unexpected EOF"),
            RowDescriptionError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
        }
    }
}

impl StdError for RowDescriptionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            RowDescriptionError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, RowDescriptionError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(RowDescriptionError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(RowDescriptionError::Utf8Error)?)
}

fn decode_format_code(code: i16) -> Result<ResultFormat, RowDescriptionError> {
    match code {
        0 => Ok(ResultFormat::Text),
        1 => Ok(ResultFormat::Binary),
        other => Err(RowDescriptionError::InvalidFormatCode(other)),
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

impl<'a> WireSerializable<'a> for RowDescriptionFrame<'a> {
    type Error = RowDescriptionError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(RowDescriptionError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'T' {
            return Err(RowDescriptionError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            return Err(RowDescriptionError::UnexpectedLength(len));
        }

        let num_fields = bytes.get_i16() as usize;

        let mut fields = Vec::with_capacity(num_fields);

        for _ in 0..num_fields {
            let name = read_cstr(&mut bytes)?;

            if bytes.remaining() < 18 {
                return Err(RowDescriptionError::UnexpectedEof);
            }

            let table_oid = bytes.get_u32();
            let column_attr = bytes.get_i16();
            let type_oid = bytes.get_u32();
            let type_size = bytes.get_i16();
            let type_modifier = bytes.get_i32();
            let format_code = bytes.get_i16();
            let format = decode_format_code(format_code)?;

            fields.push(RowField {
                name,
                table_oid,
                column_attr,
                type_oid,
                type_size,
                type_modifier,
                format,
            });
        }

        if bytes.has_remaining() {
            return Err(RowDescriptionError::UnexpectedLength(len));
        }

        Ok(RowDescriptionFrame { fields })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            body.extend_from_slice(field.name.as_bytes());
            body.put_u8(0);
            body.put_u32(field.table_oid);
            body.put_i16(field.column_attr);
            body.put_u32(field.type_oid);
            body.put_i16(field.type_size);
            body.put_i32(field.type_modifier);
            encode_format_code(&mut body, field.format);
        }

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'T');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        2 + self
            .fields
            .iter()
            .map(|f| f.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2)
            .sum::<usize>()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> RowDescriptionFrame<'a> {
        RowDescriptionFrame {
            fields: vec![
                RowField {
                    name: "id",
                    table_oid: 16384,
                    column_attr: 1,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format: ResultFormat::Text,
                },
                RowField {
                    name: "name",
                    table_oid: 16384,
                    column_attr: 2,
                    type_oid: 25,
                    type_size: -1,
                    type_modifier: -1,
                    format: ResultFormat::Text,
                },
            ],
        }
    }

    fn make_binary_frame<'a>() -> RowDescriptionFrame<'a> {
        RowDescriptionFrame {
            fields: vec![RowField {
                name: "value",
                table_oid: 0,
                column_attr: 0,
                type_oid: 17,
                type_size: -1,
                type_modifier: -1,
                format: ResultFormat::Binary,
            }],
        }
    }

    #[test]
    fn serialize_row_description() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"T\x00\x00\x00\x2F\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_row_description() {
        let data = b"T\x00\x00\x00\x2F\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00";
        let frame = RowDescriptionFrame::from_bytes(data).unwrap();
        assert_eq!(frame.fields.len(), 2);
        assert_eq!(frame.fields[0].name, "id");
        assert_eq!(frame.fields[0].table_oid, 16384);
        assert_eq!(frame.fields[0].column_attr, 1);
        assert_eq!(frame.fields[0].type_oid, 23);
        assert_eq!(frame.fields[0].type_size, 4);
        assert_eq!(frame.fields[0].type_modifier, -1);
        assert_eq!(frame.fields[0].format, ResultFormat::Text);
        assert_eq!(frame.fields[1].name, "name");
        assert_eq!(frame.fields[1].table_oid, 16384);
        assert_eq!(frame.fields[1].column_attr, 2);
        assert_eq!(frame.fields[1].type_oid, 25);
        assert_eq!(frame.fields[1].type_size, -1);
        assert_eq!(frame.fields[1].type_modifier, -1);
        assert_eq!(frame.fields[1].format, ResultFormat::Text);
    }

    #[test]
    fn roundtrip_row_description() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = RowDescriptionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn roundtrip_binary() {
        let original = make_binary_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = RowDescriptionFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn invalid_tag() {
        let data = b"U\x00\x00\x00\x2F\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00";
        let err = RowDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, RowDescriptionError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"T\x00\x00\x00\x30\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00";
        let err = RowDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, RowDescriptionError::UnexpectedLength(_));
    }

    #[test]
    fn missing_nul_in_name() {
        let data = b"T\x00\x00\x00\x2F\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name";
        let err = RowDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, RowDescriptionError::UnexpectedEof);
    }

    #[test]
    fn invalid_utf8_name() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[7] = 0xFF; // corrupt name byte
        let err = RowDescriptionFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, RowDescriptionError::Utf8Error(_));
    }

    #[test]
    fn invalid_format_code() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[25] = 0x00;
        bytes[26] = 0x02; // invalid format 2
        let err = RowDescriptionFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, RowDescriptionError::InvalidFormatCode(2));
    }

    #[test]
    fn short_field_data() {
        let data = b"T\x00\x00\x00\x2E\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00"; // one byte short
        let err = RowDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, RowDescriptionError::UnexpectedEof);
    }

    #[test]
    fn extra_data() {
        let data = b"T\x00\x00\x00\x2F\x00\x02id\x00\x00\x00\x40\x00\x00\x01\x00\x00\x00\x17\x00\x04\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\x00\x40\x00\x00\x02\x00\x00\x00\x19\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00\x00";
        let err = RowDescriptionFrame::from_bytes(data).unwrap_err();
        matches!(err, RowDescriptionError::UnexpectedLength(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
