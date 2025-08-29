//! Module: wire_protocol::backend::command_complete
//!
//! Provides parsing and serialization for the CommandComplete message ('C') in the protocol.
//!
//! - `CommandCompleteFrame`: represents the CommandComplete message with the command tag.
//! - `CommandCompleteError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `CommandCompleteFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CommandCompleteFrame<'a> {
    pub command_tag: CommandTag<'a>,
}

// -----------------------------------------------------------------------------
// ----- Subproperties ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum CommandTag<'a> {
    Insert { oid: u32, rows: u64 },
    Delete { rows: u64 },
    Update { rows: u64 },
    Merge { rows: u64 },
    Select { rows: u64 },
    Move { rows: u64 },
    Fetch { rows: u64 },
    Copy { rows: u64 },
    Other { tag: &'a str },
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CommandCompleteError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidCommandTagFormat,
}

impl fmt::Display for CommandCompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandCompleteError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            CommandCompleteError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CommandCompleteError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            CommandCompleteError::UnexpectedEof => write!(f, "unexpected EOF"),
            CommandCompleteError::InvalidCommandTagFormat => {
                write!(f, "invalid command tag format")
            }
        }
    }
}

impl StdError for CommandCompleteError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            CommandCompleteError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(bytes: &'a [u8]) -> Result<(&'a str, usize), CommandCompleteError> {
    let nul = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or(CommandCompleteError::UnexpectedEof)?;

    let raw = &bytes[..nul];
    let s = str::from_utf8(raw).map_err(CommandCompleteError::Utf8Error)?;

    Ok((s, nul + 1))
}

fn num_digits(mut num: u64) -> usize {
    if num == 0 {
        1
    } else {
        let mut count = 0;
        while num > 0 {
            num /= 10;
            count += 1;
        }
        count
    }
}

fn write_num(buf: &mut BytesMut, mut num: u64) {
    if num == 0 {
        buf.put_u8(b'0');
        return;
    }

    let mut digits = [0u8; 20];
    let mut i = digits.len() - 1;
    while num > 0 {
        digits[i] = b'0' + (num % 10) as u8;
        num /= 10;
        i -= 1;
    }
    buf.extend_from_slice(&digits[i + 1..]);
}

impl<'a> CommandTag<'a> {
    fn parse(tag: &'a str) -> Result<Self, CommandCompleteError> {
        let parts: Vec<&str> = tag.split_whitespace().collect();
        match parts.as_slice() {
            ["INSERT", oid_str, rows_str] => {
                let oid = oid_str
                    .parse::<u32>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Insert { oid, rows })
            }
            ["DELETE", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Delete { rows })
            }
            ["UPDATE", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Update { rows })
            }
            ["MERGE", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Merge { rows })
            }
            ["SELECT", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Select { rows })
            }
            ["MOVE", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Move { rows })
            }
            ["FETCH", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Fetch { rows })
            }
            ["COPY", rows_str] => {
                let rows = rows_str
                    .parse::<u64>()
                    .map_err(|_| CommandCompleteError::InvalidCommandTagFormat)?;
                Ok(Self::Copy { rows })
            }
            _ => Ok(Self::Other { tag }),
        }
    }

    fn tag_len(&self) -> usize {
        match self {
            Self::Insert { oid, rows } => 7 + num_digits(*oid as u64) + 1 + num_digits(*rows), // "INSERT " + oid + " " + rows
            Self::Delete { rows } => 7 + num_digits(*rows), // "DELETE " + rows
            Self::Update { rows } => 7 + num_digits(*rows), // "UPDATE " + rows
            Self::Merge { rows } => 6 + num_digits(*rows),  // "MERGE " + rows
            Self::Select { rows } => 7 + num_digits(*rows), // "SELECT " + rows
            Self::Move { rows } => 5 + num_digits(*rows),   // "MOVE " + rows
            Self::Fetch { rows } => 6 + num_digits(*rows),  // "FETCH " + rows
            Self::Copy { rows } => 5 + num_digits(*rows),   // "COPY " + rows
            Self::Other { tag } => tag.len(),
        }
    }

    fn write_to(&self, buf: &mut BytesMut) {
        match self {
            Self::Insert { oid, rows } => {
                buf.extend_from_slice(b"INSERT ");
                write_num(buf, *oid as u64);
                buf.put_u8(b' ');
                write_num(buf, *rows);
            }
            Self::Delete { rows } => {
                buf.extend_from_slice(b"DELETE ");
                write_num(buf, *rows);
            }
            Self::Update { rows } => {
                buf.extend_from_slice(b"UPDATE ");
                write_num(buf, *rows);
            }
            Self::Merge { rows } => {
                buf.extend_from_slice(b"MERGE ");
                write_num(buf, *rows);
            }
            Self::Select { rows } => {
                buf.extend_from_slice(b"SELECT ");
                write_num(buf, *rows);
            }
            Self::Move { rows } => {
                buf.extend_from_slice(b"MOVE ");
                write_num(buf, *rows);
            }
            Self::Fetch { rows } => {
                buf.extend_from_slice(b"FETCH ");
                write_num(buf, *rows);
            }
            Self::Copy { rows } => {
                buf.extend_from_slice(b"COPY ");
                write_num(buf, *rows);
            }
            Self::Other { tag } => {
                buf.extend_from_slice(tag.as_bytes());
            }
        }
    }
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for CommandCompleteFrame<'a> {
    type Error = CommandCompleteError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 5 {
            return Err(CommandCompleteError::UnexpectedLength(bytes.len() as u32));
        }

        let tag = bytes[0];
        if tag != b'C' {
            return Err(CommandCompleteError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len as usize != bytes.len() - 1 {
            return Err(CommandCompleteError::UnexpectedLength(len));
        }

        let (command_tag_str, consumed) = read_cstr(&bytes[5..])?;

        if consumed != bytes.len() - 5 {
            return Err(CommandCompleteError::UnexpectedLength(len));
        }

        let command_tag = CommandTag::parse(command_tag_str)?;

        Ok(CommandCompleteFrame { command_tag })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let tag_len = self.command_tag.tag_len();
        let body_len = tag_len + 1;
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'C');
        buf.put_u32(total_len as u32);
        self.command_tag.write_to(&mut buf);
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        self.command_tag.tag_len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_select_frame() -> CommandCompleteFrame<'static> {
        CommandCompleteFrame {
            command_tag: CommandTag::Select { rows: 1 },
        }
    }

    fn make_insert_frame() -> CommandCompleteFrame<'static> {
        CommandCompleteFrame {
            command_tag: CommandTag::Insert { oid: 0, rows: 1 },
        }
    }

    fn make_begin_frame() -> CommandCompleteFrame<'static> {
        CommandCompleteFrame {
            command_tag: CommandTag::Other { tag: "BEGIN" },
        }
    }

    #[test]
    fn serialize_command_complete() {
        let frame = make_select_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"C\x00\x00\x00\x0DSELECT 1\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_command_complete() {
        let data = b"C\x00\x00\x00\x0DSELECT 1\x00";
        let frame = CommandCompleteFrame::from_bytes(data).unwrap();
        assert_eq!(frame.command_tag, CommandTag::Select { rows: 1 });
    }

    #[test]
    fn roundtrip_command_complete() {
        let original = make_select_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CommandCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.command_tag, original.command_tag);
    }

    #[test]
    fn roundtrip_insert() {
        let original = make_insert_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CommandCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.command_tag, original.command_tag);
    }

    #[test]
    fn roundtrip_other() {
        let original = make_begin_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = CommandCompleteFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.command_tag, original.command_tag);
    }

    #[test]
    fn invalid_tag() {
        let data = b"Q\x00\x00\x00\x0DSELECT 1\x00";
        let err = CommandCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CommandCompleteError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"C\x00\x00\x00\x0ESELECT 1\x00";
        let err = CommandCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CommandCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn missing_null_terminator() {
        let data = b"C\x00\x00\x00\x0DSELECT 1";
        let err = CommandCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CommandCompleteError::UnexpectedEof);
    }

    #[test]
    fn extra_data_after_null() {
        let data = b"C\x00\x00\x00\x0DSELECT 1\x00extra";
        let err = CommandCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CommandCompleteError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8() {
        let mut bytes = make_select_frame().to_bytes().unwrap().to_vec();
        bytes[5] = 0xFF; // corrupt first byte
        let err = CommandCompleteFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, CommandCompleteError::Utf8Error(_));
    }

    #[test]
    fn invalid_command_tag_format() {
        let data = b"C\x00\x00\x00\x0CDELETE abc\x00";
        let err = CommandCompleteFrame::from_bytes(data).unwrap_err();
        matches!(err, CommandCompleteError::InvalidCommandTagFormat);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
