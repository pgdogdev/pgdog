//! Module: wire_protocol::backend::authentication_gss_continue
//!
//! Provides parsing and serialization for the AuthenticationGSSContinue message ('R' with code 8) in the protocol.
//!
//! - `AuthenticationGssContinueFrame`: represents the AuthenticationGSSContinue message with GSS/SSPI continuation data.
//! - `AuthenticationGssContinueError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `AuthenticationGssContinueFrame`.

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationGssContinueFrame<'a> {
    pub data: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum AuthenticationGssContinueError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    UnexpectedAuthType(i32),
    UnexpectedEof,
}

impl fmt::Display for AuthenticationGssContinueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationGssContinueError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            AuthenticationGssContinueError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            AuthenticationGssContinueError::UnexpectedAuthType(t) => {
                write!(f, "unexpected auth type: {t}")
            }
            AuthenticationGssContinueError::UnexpectedEof => write!(f, "unexpected EOF"),
        }
    }
}

impl StdError for AuthenticationGssContinueError {}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for AuthenticationGssContinueFrame<'a> {
    type Error = AuthenticationGssContinueError;

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 9 {
            return Err(AuthenticationGssContinueError::UnexpectedEof);
        }

        let tag = bytes[0];
        if tag != b'R' {
            return Err(AuthenticationGssContinueError::UnexpectedTag(tag));
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if len < 8 {
            return Err(AuthenticationGssContinueError::UnexpectedLength(len));
        }

        if bytes.len() != 1 + len as usize {
            return Err(AuthenticationGssContinueError::UnexpectedLength(
                bytes.len() as u32,
            ));
        }

        let auth_type = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
        if auth_type != 8 {
            return Err(AuthenticationGssContinueError::UnexpectedAuthType(
                auth_type,
            ));
        }

        let data_len = (len - 8) as usize;
        let buf = &bytes[9..];
        if buf.len() < data_len {
            return Err(AuthenticationGssContinueError::UnexpectedEof);
        }

        let data = &buf[0..data_len];

        Ok(AuthenticationGssContinueFrame { data })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let body_len = 4 + self.data.len();
        let total_len = 4 + body_len;

        let mut buf = BytesMut::with_capacity(1 + total_len);
        buf.put_u8(b'R');
        buf.put_u32(total_len as u32);
        buf.put_i32(8);
        buf.extend_from_slice(self.data);

        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self.data.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(data: &[u8]) -> AuthenticationGssContinueFrame {
        AuthenticationGssContinueFrame { data }
    }

    #[test]
    fn serialize_authentication_gss_continue_empty() {
        let frame = make_frame(&[]);
        let bytes = frame.to_bytes().unwrap();
        let expected = b"R\x00\x00\x00\x08\x00\x00\x00\x08";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn serialize_authentication_gss_continue_with_data() {
        let data = b"some_gss_data";
        let frame = make_frame(data);
        let bytes = frame.to_bytes().unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(b"R");
        expected.extend_from_slice(&((4 + 4 + data.len()) as u32).to_be_bytes());
        expected.extend_from_slice(&8i32.to_be_bytes());
        expected.extend_from_slice(data);
        assert_eq!(bytes.as_ref(), expected.as_slice());
    }

    #[test]
    fn deserialize_authentication_gss_continue_empty() {
        let data_bytes = b"R\x00\x00\x00\x08\x00\x00\x00\x08";
        let frame = AuthenticationGssContinueFrame::from_bytes(data_bytes).unwrap();
        let data = frame.data;
        let expected_data: &[u8] = &[];
        assert_eq!(data, expected_data);
    }

    #[test]
    fn deserialize_authentication_gss_continue_with_data() {
        let payload = b"some_gss_data";
        let mut data_bytes = Vec::new();
        data_bytes.extend_from_slice(b"R");
        data_bytes.extend_from_slice(&((4 + 4 + payload.len()) as u32).to_be_bytes());
        data_bytes.extend_from_slice(&8i32.to_be_bytes());
        data_bytes.extend_from_slice(payload);
        let frame = AuthenticationGssContinueFrame::from_bytes(&data_bytes).unwrap();
        assert_eq!(frame.data, payload);
    }

    #[test]
    fn roundtrip_authentication_gss_continue_empty() {
        let original = make_frame(&[]);
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationGssContinueFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original.data, decoded.data);
    }

    #[test]
    fn roundtrip_authentication_gss_continue_with_data() {
        let data = b"test_data_123";
        let original = make_frame(data);
        let bytes = original.to_bytes().unwrap();
        let decoded = AuthenticationGssContinueFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(original.data, decoded.data);
    }

    #[test]
    fn invalid_tag() {
        let data = b"X\x00\x00\x00\x08\x00\x00\x00\x08";
        let err = AuthenticationGssContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssContinueError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length_short() {
        let data = b"R\x00\x00\x00\x07\x00\x00\x00\x08";
        let err = AuthenticationGssContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssContinueError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_length_mismatch() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x08\x00"; // extra byte
        let err = AuthenticationGssContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssContinueError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_auth_type() {
        let data = b"R\x00\x00\x00\x08\x00\x00\x00\x07";
        let err = AuthenticationGssContinueFrame::from_bytes(data).unwrap_err();
        matches!(err, AuthenticationGssContinueError::UnexpectedAuthType(7));
    }

    #[test]
    fn unexpected_eof() {
        let data = b"R\x00\x00\x00\x0D\x00\x00\x00\x08abc"; // len=13, but only 3 data bytes
        let err = AuthenticationGssContinueFrame::from_bytes(&data[0..12]).unwrap_err(); // truncate
        matches!(err, AuthenticationGssContinueError::UnexpectedEof);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
