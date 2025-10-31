//! Module: wire_protocol::frontend::describe_portal
//!
//! Purpose: “Describe this portal so I know its row layout.”

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribePortalFrame<'a> {
    pub name: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum DescribePortalError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    NoTerminator,
    InvalidUtf8(std::str::Utf8Error),
}

impl fmt::Display for DescribePortalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            Self::UnexpectedLength(n) => write!(f, "unexpected length: {}", n),
            Self::NoTerminator => write!(f, "missing null terminator"),
            Self::InvalidUtf8(e) => write!(f, "invalid UTF-8: {}", e),
        }
    }
}
impl StdError for DescribePortalError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        if let Self::InvalidUtf8(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for DescribePortalFrame<'a> {
    type Error = DescribePortalError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 7 {
            return Err(Self::Error::UnexpectedLength(bytes.len() as u32));
        }
        if bytes[0] != b'D' {
            return Err(Self::Error::UnexpectedTag(bytes[0]));
        }
        let declared = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if declared as usize != bytes.len() - 1 {
            return Err(Self::Error::UnexpectedLength(declared));
        }
        if bytes[5] != b'P' {
            return Err(Self::Error::UnexpectedTag(bytes[5]));
        }

        let rest = &bytes[6..];
        let nul = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(Self::Error::NoTerminator)?;
        if nul + 1 != rest.len() {
            return Err(Self::Error::NoTerminator);
        }

        let name = std::str::from_utf8(&rest[..nul]).map_err(Self::Error::InvalidUtf8)?;
        Ok(Self { name })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let n = self.name.as_bytes();
        let body_len = 1 + n.len() + 1;
        let total = 4 + body_len as u32;

        let mut buf = BytesMut::with_capacity(1 + total as usize);
        buf.put_u8(b'D');
        buf.put_u32(total);
        buf.put_u8(b'P');
        buf.put_slice(n);
        buf.put_u8(0);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        1 + self.name.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(name: &str) -> DescribePortalFrame {
        DescribePortalFrame { name }
    }

    #[test]
    fn empty_name() {
        let empty_name = "";
        let frame = make_frame(empty_name);
        let encoded = frame.to_bytes().unwrap();
        let decoded = DescribePortalFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(frame, decoded);
        assert_eq!(frame.name, empty_name);
    }

    #[test]
    fn named() {
        let name = "port";
        let frame = make_frame(name);
        let encoded = frame.to_bytes().unwrap();
        let decoded = DescribePortalFrame::from_bytes(encoded.as_ref()).unwrap();
        assert_eq!(frame, decoded);
        assert_eq!(frame.name, name);
    }

    #[test]
    fn bad_tag_not_describe() {
        let name = "_pgdog_portal";
        let mut buf = make_frame(name).to_bytes().unwrap().to_vec();
        buf[0] = b'X'; // corrupt the tag

        assert!(matches!(
            DescribePortalFrame::from_bytes(&buf),
            Err(DescribePortalError::UnexpectedTag(b'X'))
        ));
    }

    #[test]
    fn bad_kind_tag() {
        let name = "_pgdog_portal";
        let mut buf = make_frame(name).to_bytes().unwrap().to_vec();
        buf[5] = b'S'; // overwrite the kind byte ('P' → 'S')

        assert!(matches!(
            DescribePortalFrame::from_bytes(&buf),
            Err(DescribePortalError::UnexpectedTag(b'S'))
        ));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
