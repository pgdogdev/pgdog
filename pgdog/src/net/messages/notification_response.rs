use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use crate::net::c_string_buf_len;

use super::code;
use super::prelude::*;

/// NotificationResponse (B).
#[derive(Debug, Clone)]
pub(crate) struct NotificationResponse {
    payload: Bytes,
    channel_len: usize,
    pid: i32,
}

impl NotificationResponse {
    /// Get the name of the notification channel.
    pub(crate) fn channel(&self) -> &str {
        let start = 1 + 4 + 4;
        let end = start + self.channel_len - 1;

        unsafe { from_utf8_unchecked(&self.payload[start..end]) }
    }

    /// Get message payload.
    pub(crate) fn payload(&self) -> &str {
        let start = 1 + 4 + 4 + self.channel_len;
        unsafe { from_utf8_unchecked(&self.payload[start..self.payload.len() - 1]) }
    }

    /// Which connection sent the notification.
    pub(crate) fn pid(&self) -> i32 {
        self.pid
    }
}

impl FromBytes for NotificationResponse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let payload = bytes.clone();

        code!(bytes, 'A');
        let _len = bytes.get_i32();

        let pid = bytes.get_i32();
        let channel_len = c_string_buf_len(&bytes[..]);
        from_utf8(&bytes[..channel_len - 1])?;
        bytes.advance(channel_len);
        from_utf8(&bytes[..bytes.len() - 1])?;

        Ok(Self {
            channel_len,
            pid,
            payload,
        })
    }
}

impl ToBytes for NotificationResponse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
    }
}

impl Protocol for NotificationResponse {
    fn code(&self) -> char {
        'A'
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use bytes::BufMut;

    use crate::net::{FromBytes, Payload};

    #[test]
    fn test_notification_response() {
        let mut bytes = Payload::named('A');
        bytes.put_i32(1234); // pid
        bytes.put_string("channel_name");
        bytes.put_string("payload");

        let payload = bytes.freeze();
        let notification = NotificationResponse::from_bytes(payload).unwrap();
        assert_eq!(notification.channel(), "channel_name");
        assert_eq!(notification.payload(), "payload");
    }
}
