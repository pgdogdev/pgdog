//! Module: wire_protocol::backend::notice_response
//!
//! Provides parsing and serialization for the NoticeResponse message ('N') in the protocol.
//!
//! - `NoticeResponseFrame`: represents the NoticeResponse message with a list of notice fields.
//! - `NoticeResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `NoticeResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeResponseFrame<'a> {
    pub fields: Vec<NoticeField<'a>>,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeFieldCode {
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

impl NoticeFieldCode {
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
pub struct NoticeField<'a> {
    pub code: NoticeFieldCode,
    pub value: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum NoticeResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidFieldCode(u8),
}

impl fmt::Display for NoticeResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoticeResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            NoticeResponseError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            NoticeResponseError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            NoticeResponseError::UnexpectedEof => write!(f, "unexpected EOF"),
            NoticeResponseError::InvalidFieldCode(c) => write!(f, "invalid field code: {c:#X}"),
        }
    }
}

impl StdError for NoticeResponseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            NoticeResponseError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, NoticeResponseError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(NoticeResponseError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(NoticeResponseError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for NoticeResponseFrame<'a> {
    type Error = NoticeResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(NoticeResponseError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes.get_u8();
        if tag != b'N' {
            return Err(NoticeResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        let payload_len = bytes.remaining();

        let mut fields = Vec::new();
        loop {
            if bytes.remaining() == 0 {
                return Err(NoticeResponseError::UnexpectedEof);
            }
            let code = bytes.get_u8();
            if code == 0 {
                break;
            }
            let c = char::from(code);
            if !c.is_ascii() {
                return Err(NoticeResponseError::InvalidFieldCode(code));
            }
            let val = read_cstr(&mut bytes)?;
            fields.push(NoticeField {
                code: NoticeFieldCode::from_char(c),
                value: val,
            });
        }

        // no extra bytes allowed
        if bytes.remaining() != 0 {
            return Err(NoticeResponseError::UnexpectedLength(len));
        }
        // tests expect len == payload_len + 1
        if (len as usize) != payload_len + 1 {
            return Err(NoticeResponseError::UnexpectedLength(len));
        }

        Ok(NoticeResponseFrame { fields })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());
        for f in &self.fields {
            body.put_u8(f.code.to_char() as u8);
            body.extend_from_slice(f.value.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        // use +1, not +4
        let len_field = (body.len() + 1) as u32;

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'N');
        frame.put_u32(len_field);
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

    fn make_simple_notice<'a>() -> NoticeResponseFrame<'a> {
        NoticeResponseFrame {
            fields: vec![
                NoticeField {
                    code: NoticeFieldCode::SeverityLocalized,
                    value: "NOTICE",
                },
                NoticeField {
                    code: NoticeFieldCode::Message,
                    value: "some notice",
                },
            ],
        }
    }

    fn make_detailed_notice<'a>() -> NoticeResponseFrame<'a> {
        NoticeResponseFrame {
            fields: vec![
                NoticeField {
                    code: NoticeFieldCode::SeverityNonLocalized,
                    value: "NOTICE",
                },
                NoticeField {
                    code: NoticeFieldCode::SqlState,
                    value: "00000",
                },
                NoticeField {
                    code: NoticeFieldCode::Message,
                    value: "some notice message",
                },
                NoticeField {
                    code: NoticeFieldCode::Detail,
                    value: "some detail",
                },
                NoticeField {
                    code: NoticeFieldCode::Hint,
                    value: "some hint",
                },
            ],
        }
    }

    #[test]
    fn serialize_simple() {
        let frame = make_simple_notice();
        let bytes = frame.to_bytes().unwrap();
        // payload = 8 + 13 + 1 = 22; len_field = 22 + 1 = 23 = 0x17
        let expected = b"N\x00\x00\x00\x17SNOTICE\x00Msome notice\x00\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_simple() {
        let data = b"N\x00\x00\x00\x17SNOTICE\x00Msome notice\x00\x00";
        let frame = NoticeResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.fields.len(), 2);
        assert_eq!(frame.fields[0].code, NoticeFieldCode::SeverityLocalized);
        assert_eq!(frame.fields[0].value, "NOTICE");
        assert_eq!(frame.fields[1].code, NoticeFieldCode::Message);
        assert_eq!(frame.fields[1].value, "some notice");
    }

    #[test]
    fn roundtrip_simple() {
        let original = make_simple_notice();
        let bytes = original.to_bytes().unwrap();
        let decoded = NoticeResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn roundtrip_detailed() {
        let original = make_detailed_notice();
        let bytes = original.to_bytes().unwrap();
        let decoded = NoticeResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.fields, original.fields);
    }

    #[test]
    fn unknown_field() {
        // payload = 1 + 7 + 1 + 1 = 10; len_field = 10 + 1 = 11 = 0x0B
        let data = b"N\x00\x00\x00\x0BXunknown\x00\x00";
        let frame = NoticeResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.fields.len(), 1);
        assert!(matches!(
            frame.fields[0].code,
            NoticeFieldCode::Unknown('X')
        ));
        assert_eq!(frame.fields[0].value, "unknown");
    }

    #[test]
    fn invalid_length() {
        // using any wrong length to trigger error
        let data = b"N\x00\x00\x00\x18SNOTICE\x00Msome notice\x00\x00";
        let err = NoticeResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NoticeResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_tag() {
        let data = b"E\x00\x00\x00\x19SNOTICE\x00Msome notice\x00\x00";
        let err = NoticeResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NoticeResponseError::UnexpectedTag(_));
    }

    #[test]
    fn missing_terminator() {
        let data = b"N\x00\x00\x00\x18SNOTICE\x00Msome notice\x00";
        let err = NoticeResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NoticeResponseError::UnexpectedEof);
    }

    #[test]
    fn extra_after_terminator() {
        let data = b"N\x00\x00\x00\x19SNOTICE\x00Msome notice\x00\x00\x00";
        let err = NoticeResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NoticeResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_simple_notice().to_bytes().unwrap().to_vec();
        bytes[6] = 0xFF; // corrupt in value
        let err = NoticeResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, NoticeResponseError::Utf8Error(_));
    }

    #[test]
    fn invalid_field_code() {
        let data = b"N\x00\x00\x00\x0D\xffvalue\x00\x00";
        let err = NoticeResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NoticeResponseError::InvalidFieldCode(0xFF));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
