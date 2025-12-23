//! Module: wire_protocol::backend::error_response
//!
//! Provides parsing and serialization for the ErrorResponse message ('E') in the protocol.
//!
//! - `ErrorResponseFrame`: represents the ErrorResponse message with a list of error fields.
//! - `ErrorResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ErrorResponseFrame`.

use crate::wire_protocol::WireSerializable;
use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponseFrame<'a> {
    pub fields: Vec<ErrorField<'a>>,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFieldCode {
    SeverityLocalized,
    SeverityNonLocalized,
    SqlState,
    Message,
    Detail,
    Hint,
    Position,
    InternalPosition,
    InternalQuery,
    Where,
    SchemaName,
    TableName,
    ColumnName,
    DataTypeName,
    ConstraintName,
    File,
    Line,
    Routine,
    Unknown(char),
}

impl ErrorFieldCode {
    pub fn from_char(c: char) -> Self {
        match c {
            'S' => Self::SeverityLocalized,
            'V' => Self::SeverityNonLocalized,
            'C' => Self::SqlState,
            'M' => Self::Message,
            'D' => Self::Detail,
            'H' => Self::Hint,
            'P' => Self::Position,
            'p' => Self::InternalPosition,
            'q' => Self::InternalQuery,
            'W' => Self::Where,
            's' => Self::SchemaName,
            't' => Self::TableName,
            'c' => Self::ColumnName,
            'd' => Self::DataTypeName,
            'n' => Self::ConstraintName,
            'F' => Self::File,
            'L' => Self::Line,
            'R' => Self::Routine,
            other => Self::Unknown(other),
        }
    }

    pub fn to_char(&self) -> char {
        match self {
            Self::SeverityLocalized => 'S',
            Self::SeverityNonLocalized => 'V',
            Self::SqlState => 'C',
            Self::Message => 'M',
            Self::Detail => 'D',
            Self::Hint => 'H',
            Self::Position => 'P',
            Self::InternalPosition => 'p',
            Self::InternalQuery => 'q',
            Self::Where => 'W',
            Self::SchemaName => 's',
            Self::TableName => 't',
            Self::ColumnName => 'c',
            Self::DataTypeName => 'd',
            Self::ConstraintName => 'n',
            Self::File => 'F',
            Self::Line => 'L',
            Self::Routine => 'R',
            Self::Unknown(c) => *c,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorField<'a> {
    pub code: ErrorFieldCode,
    pub value: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ErrorResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidFieldCode(u8),
}

impl fmt::Display for ErrorResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            ErrorResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            ErrorResponseError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            ErrorResponseError::UnexpectedEof => write!(f, "unexpected EOF"),
            ErrorResponseError::InvalidFieldCode(c) => write!(f, "invalid field code: {c:#X}"),
        }
    }
}

impl StdError for ErrorResponseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        if let ErrorResponseError::Utf8Error(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ErrorResponseFrame<'a> {
    type Error = ErrorResponseError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        // Need at least tag + length
        if bytes.len() < 5 {
            return Err(ErrorResponseError::UnexpectedLength(bytes.len() as u32));
        }
        // Tag check
        if bytes[0] != b'E' {
            return Err(ErrorResponseError::UnexpectedTag(bytes[0]));
        }
        // Read length field
        let len_field = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        // The rest is payload
        let payload = &bytes[5..];
        let payload_len = payload.len();

        // Parse fields
        let mut offset = 0;
        let mut fields = Vec::new();
        let mut seen_terminator = false;

        while offset < payload_len {
            let code_byte = payload[offset];
            offset += 1;

            // final terminator
            if code_byte == 0 {
                seen_terminator = true;
                break;
            }
            let code_char = code_byte as char;
            if !code_char.is_ascii() {
                return Err(ErrorResponseError::InvalidFieldCode(code_byte));
            }

            // find the NUL ending this field
            let rest = &payload[offset..];
            let pos = rest
                .iter()
                .position(|&b| b == 0)
                .ok_or(ErrorResponseError::UnexpectedEof)?;
            let raw = &rest[..pos];
            let value = str::from_utf8(raw).map_err(ErrorResponseError::Utf8Error)?;

            // advance past the field data + its NUL
            offset += pos + 1;

            fields.push(ErrorField {
                code: ErrorFieldCode::from_char(code_char),
                value,
            });
        }

        if !seen_terminator {
            // we ran out of bytes without hitting the 0
            return Err(ErrorResponseError::UnexpectedEof);
        }

        // No extra bytes allowed after the terminator
        if offset != payload_len {
            return Err(ErrorResponseError::UnexpectedLength(len_field));
        }

        // Tests expect len_field == payload_len + 1
        if (len_field as usize) != payload_len + 1 {
            return Err(ErrorResponseError::UnexpectedLength(len_field));
        }

        Ok(ErrorResponseFrame { fields })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        // build payload: each field + its NUL, then final 0
        let mut body = BytesMut::with_capacity(self.body_size());
        for field in &self.fields {
            body.put_u8(field.code.to_char() as u8);
            body.extend_from_slice(field.value.as_bytes());
            body.put_u8(0);
        }
        // final terminator
        body.put_u8(0);

        // length = payload.len() + 1 (per your tests)
        let len_field = (body.len() + 1) as u32;

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'E');
        frame.put_u32(len_field);
        frame.extend_from_slice(&body);
        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        // sum of (code + value + NUL) for each field, plus one final terminator
        1 + self
            .fields
            .iter()
            .map(|f| 1 + f.value.len() + 1)
            .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_error<'a>() -> ErrorResponseFrame<'a> {
        ErrorResponseFrame {
            fields: vec![
                ErrorField {
                    code: ErrorFieldCode::SeverityLocalized,
                    value: "ERROR",
                },
                ErrorField {
                    code: ErrorFieldCode::Message,
                    value: "permission denied",
                },
            ],
        }
    }

    fn make_detailed_error<'a>() -> ErrorResponseFrame<'a> {
        ErrorResponseFrame {
            fields: vec![
                ErrorField {
                    code: ErrorFieldCode::SeverityNonLocalized,
                    value: "ERROR",
                },
                ErrorField {
                    code: ErrorFieldCode::SqlState,
                    value: "42501",
                },
                ErrorField {
                    code: ErrorFieldCode::Message,
                    value: "permission denied for table test",
                },
                ErrorField {
                    code: ErrorFieldCode::Detail,
                    value: "some detail",
                },
                ErrorField {
                    code: ErrorFieldCode::Hint,
                    value: "grant permission",
                },
            ],
        }
    }

    #[test]
    fn serialize_simple() {
        let frame = make_simple_error();
        let bytes = frame.to_bytes().unwrap();
        // length = payload.len() + 1 = 28 = 0x1C
        let expected = b"E\x00\x00\x00\x1CSERROR\x00Mpermission denied\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_simple() {
        let data = b"E\x00\x00\x00\x1CSERROR\x00Mpermission denied\x00\x00";
        let frame = ErrorResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.fields.len(), 2);
        assert_eq!(frame.fields[0].code, ErrorFieldCode::SeverityLocalized);
        assert_eq!(frame.fields[0].value, "ERROR");
        assert_eq!(frame.fields[1].code, ErrorFieldCode::Message);
        assert_eq!(frame.fields[1].value, "permission denied");
    }

    #[test]
    fn roundtrip_simple() {
        let original = make_simple_error();
        let bytes = original.to_bytes().unwrap();
        let decoded = ErrorResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn roundtrip_detailed() {
        let original = make_detailed_error();
        let bytes = original.to_bytes().unwrap();
        let decoded = ErrorResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn unknown_field() {
        // length = payload.len() (10) + 1 = 11 = 0x0B
        let data = b"E\x00\x00\x00\x0BXunknown\x00\x00";
        let frame = ErrorResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.fields.len(), 1);
        assert!(matches!(frame.fields[0].code, ErrorFieldCode::Unknown('X')));
        assert_eq!(frame.fields[0].value, "unknown");
    }

    #[test]
    fn invalid_tag() {
        let data = b"N\x00\x00\x00\x1CSERROR\x00Mpermission denied\x00\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"E\x00\x00\x00\x1DSERROR\x00Mpermission denied\x00\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::UnexpectedLength(_));
    }

    #[test]
    fn missing_terminator() {
        let data = b"E\x00\x00\x00\x1CSERROR\x00Mpermission denied\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::UnexpectedEof);
    }

    #[test]
    fn extra_after_terminator() {
        let data = b"E\x00\x00\x00\x1CSERROR\x00Mpermission denied\x00\x00\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_simple_error().to_bytes().unwrap().to_vec();
        bytes[6] = 0xFF; // corrupt in the value portion
        let err = ErrorResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, ErrorResponseError::Utf8Error(_));
    }

    #[test]
    fn invalid_field_code() {
        let data = b"E\x00\x00\x00\x0D\xFFvalue\x00\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::InvalidFieldCode(0xFF));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
