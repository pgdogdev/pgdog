use crate::net::c_string_buf;

use super::super::super::code;
use super::super::super::prelude::*;

/// Logical decoding message. Written with `pg_logical_emit_message` (injects custom message into WAL)
/// <https://www.postgresql.org/docs/current/functions-admin.html>
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Message {
    /// "textual prefix that can be used by logical decoding plugins to easily recognize messages that are interesting for them"
    pub(crate) prefix: String,
    pub(crate) content: Bytes,
}

/// <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
impl FromBytes for Message {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'M');

        // Flags + LSN of the message.
        if bytes.remaining() < 1 + 8 {
            return Err(Error::UnexpectedPayload);
        }
        bytes.advance(1 + 8);

        let prefix = c_string_buf(&mut bytes);

        if bytes.remaining() < 4 {
            return Err(Error::UnexpectedPayload);
        }

        let length_of_content = bytes.get_u32() as usize;
        if bytes.remaining() < length_of_content {
            return Err(Error::UnexpectedPayload);
        }

        let content = bytes.split_to(length_of_content);
        Ok(Self { prefix, content })
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use super::*;

    fn message(prefix: &str, len: u32, content: &[u8]) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'M');
        bytes.put_u8(1);
        bytes.put_u64(1234);
        bytes.put_slice(prefix.as_bytes());
        bytes.put_u8(0);
        bytes.put_u32(len);
        bytes.put_slice(content);
        bytes.freeze()
    }

    #[test]
    fn parses_prefix_and_content() {
        let message = Message::from_bytes(message("ddl_replication", 5, b"hello")).unwrap();
        assert_eq!(message.prefix, "ddl_replication");
        assert_eq!(&message.content[..], b"hello");
    }

    #[test]
    fn parses_empty_content() {
        let message = Message::from_bytes(message("prefix", 0, b"")).unwrap();
        assert_eq!(message.prefix, "prefix");
        assert!(message.content.is_empty());
    }

    #[test]
    fn keeps_binary_content() {
        let message = Message::from_bytes(message("prefix", 2, &[0xff, 0x00])).unwrap();
        assert_eq!(&message.content[..], &[0xff, 0x00]);
    }

    #[test]
    fn rejects_truncated_message() {
        let full = message("ddl_replication", 5, b"hello");
        for len in 1..full.len() {
            assert!(
                Message::from_bytes(full.slice(..len)).is_err(),
                "{len} bytes of {} parsed",
                full.len()
            );
        }
    }

    #[test]
    fn rejects_content_shorter_than_its_length() {
        assert!(Message::from_bytes(message("prefix", 10, b"hello")).is_err());
    }
}
