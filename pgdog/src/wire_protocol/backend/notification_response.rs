//! Module: wire_protocol::backend::notification_response
//!
//! Provides parsing and serialization for the NotificationResponse message ('A') in the protocol.
//!
//! - `NotificationResponseFrame`: represents the NotificationResponse message with PID, channel, and payload.
//! - `NotificationResponseError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `NotificationResponseFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone)]
pub struct NotificationResponseFrame<'a> {
    pub pid: i32,
    pub channel: &'a str,
    pub payload: &'a str,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum NotificationResponseError {
    UnexpectedTag(u8),
    UnexpectedLength(u32),
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
}

impl fmt::Display for NotificationResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationResponseError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            NotificationResponseError::UnexpectedLength(len) => {
                write!(f, "unexpected length: {len}")
            }
            NotificationResponseError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            NotificationResponseError::UnexpectedEof => write!(f, "unexpected EOF"),
        }
    }
}

impl StdError for NotificationResponseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            NotificationResponseError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, NotificationResponseError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(NotificationResponseError::UnexpectedEof)?;

    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL

    Ok(str::from_utf8(raw).map_err(NotificationResponseError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for NotificationResponseFrame<'a> {
    type Error = NotificationResponseError;

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(NotificationResponseError::UnexpectedLength(
                bytes.len() as u32
            ));
        }

        let tag = bytes.get_u8();
        if tag != b'A' {
            return Err(NotificationResponseError::UnexpectedTag(tag));
        }

        let len = bytes.get_u32();
        if len as usize != bytes.remaining() + 4 {
            return Err(NotificationResponseError::UnexpectedLength(len));
        }

        let pid = bytes.get_i32();

        let channel = read_cstr(&mut bytes)?;

        let payload = read_cstr(&mut bytes)?;

        if bytes.remaining() != 0 {
            return Err(NotificationResponseError::UnexpectedLength(len));
        }

        Ok(NotificationResponseFrame {
            pid,
            channel,
            payload,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.put_i32(self.pid);
        body.extend_from_slice(self.channel.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(self.payload.as_bytes());
        body.put_u8(0);

        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'A');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        4 + self.channel.len() + 1 + self.payload.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> NotificationResponseFrame<'a> {
        NotificationResponseFrame {
            pid: 1234,
            channel: "test_channel",
            payload: "test_payload",
        }
    }

    fn make_empty_payload_frame<'a>() -> NotificationResponseFrame<'a> {
        NotificationResponseFrame {
            pid: 5678,
            channel: "empty",
            payload: "",
        }
    }

    #[test]
    fn serialize_notification() {
        let frame = make_frame();
        let bytes = frame.to_bytes().unwrap();
        let expected = b"A\x00\x00\x00\x22\x00\x00\x04\xD2test_channel\x00test_payload\x00";
        assert_eq!(bytes.as_ref(), expected);
    }

    #[test]
    fn deserialize_notification() {
        let data = b"A\x00\x00\x00\x22\x00\x00\x04\xD2test_channel\x00test_payload\x00";
        let frame = NotificationResponseFrame::from_bytes(data).unwrap();
        assert_eq!(frame.pid, 1234);
        assert_eq!(frame.channel, "test_channel");
        assert_eq!(frame.payload, "test_payload");
    }

    #[test]
    fn roundtrip_notification() {
        let original = make_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = NotificationResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.pid, original.pid);
        assert_eq!(decoded.channel, original.channel);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn roundtrip_empty_payload() {
        let original = make_empty_payload_frame();
        let bytes = original.to_bytes().unwrap();
        let decoded = NotificationResponseFrame::from_bytes(bytes.as_ref()).unwrap();
        assert_eq!(decoded.pid, original.pid);
        assert_eq!(decoded.channel, original.channel);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn invalid_tag() {
        let data = b"B\x00\x00\x00\x22\x00\x00\x04\xD2test_channel\x00test_payload\x00";
        let err = NotificationResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NotificationResponseError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_length() {
        let data = b"A\x00\x00\x00\x23\x00\x00\x04\xD2test_channel\x00test_payload\x00";
        let err = NotificationResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NotificationResponseError::UnexpectedLength(_));
    }

    #[test]
    fn missing_channel_nul() {
        let data = b"A\x00\x00\x00\x1A\x00\x00\x04\xD2test_channeltest_payload\x00";
        let err = NotificationResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NotificationResponseError::UnexpectedEof);
    }

    #[test]
    fn missing_payload_nul() {
        let data = b"A\x00\x00\x00\x21\x00\x00\x04\xD2test_channel\x00test_payload";
        let err = NotificationResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NotificationResponseError::UnexpectedEof);
    }

    #[test]
    fn extra_data() {
        let data = b"A\x00\x00\x00\x22\x00\x00\x04\xD2test_channel\x00test_payload\x00\x00";
        let err = NotificationResponseFrame::from_bytes(data).unwrap_err();
        matches!(err, NotificationResponseError::UnexpectedLength(_));
    }

    #[test]
    fn invalid_utf8_channel() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[9] = 0xFF; // corrupt channel byte
        let err = NotificationResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, NotificationResponseError::Utf8Error(_));
    }

    #[test]
    fn invalid_utf8_payload() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[22] = 0xFF; // corrupt payload byte
        let err = NotificationResponseFrame::from_bytes(&bytes).unwrap_err();
        matches!(err, NotificationResponseError::Utf8Error(_));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
