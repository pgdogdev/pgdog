//! Module: wire_protocol::backend::error_response
//!
//! Provides parsing and serialization for the ErrorResponse message ('E') in the protocol.
//!
//! - `ErrorResponseFrame`: represents the ErrorResponse message with a list of error fields.
//! - `ErrorResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `ErrorResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

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
        match self {
            ErrorResponseError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, ErrorResponseError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(ErrorResponseError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(ErrorResponseError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for ErrorResponseFrame<'a> {
    type Error = ErrorResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(ErrorResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'E' {
            return Err(ErrorResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            return Err(ErrorResponseError::UnexpectedLength(len));
        }

        let mut fields = Vec::new();

        loop {
            if bytes.remaining() == 0 {
                return Err(ErrorResponseError::UnexpectedEof);
            }
            let code_byte = bytes.get_u8();
            if code_byte == 0 {
                break;
            }
            let code_char = char::from(code_byte);
            if !code_char.is_ascii() {
                return Err(ErrorResponseError::InvalidFieldCode(code_byte));
            }
            let value = read_cstr(&mut bytes)?;
            let code = ErrorFieldCode::from_char(code_char);
            fields.push(ErrorField { code, value });
        }

        if bytes.remaining() != 0 {
            return Err(ErrorResponseError::UnexpectedLength(len));
        }

        Ok(ErrorResponseFrame { fields })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        for field in &self.fields {
            let code_char = field.code.to_char();
            body.put_u8(code_char as u8);
            body.extend_from_slice(field.value.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'E');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        let mut size = 1; // terminator
        for field in &self.fields {
            size += 1 + field.value.len() + 1; // code + value + nul
        }
        size
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

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
        let data = b"E\x00\x00\x00\x11Xunknown\x00\x00";
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
        bytes[6] = 0xFF; // corrupt in value
        let err = ErrorResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, ErrorResponseError::Utf8Error(_));
    }

    #[test]
    fn invalid_field_code() {
        let data = b"E\x00\x00\x00\x0D\xffvalue\x00\x00";
        let err = ErrorResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, ErrorResponseError::InvalidFieldCode(0xFF));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
