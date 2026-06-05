use tracing::debug;

use crate::net::{FromBytes, Message};

const HEADER_CODE_LEN: usize = 1;
const HEADER_LEN_SIZE: usize = 4;
const HEADER_TOTAL: usize = HEADER_CODE_LEN + HEADER_LEN_SIZE;

/// Deserializes a flat byte blob (N concatenated PostgreSQL wire messages) into `Vec<Message>`.
///
/// Redis stores cache responses as raw wire-format bytes concatenated together without framing.
/// We walk through the blob reading each message boundary, then slice out the individual message.
///
/// ### PostgreSQL wire protocol message layout:
///
/// [Source](https://www.postgresql.org/docs/current/protocol-overview.html)
///
/// ```text
/// +----------+--------------------------+-------------------+
/// | 1 byte   | 4 bytes (big-endian)     | N bytes (payload) |
/// | code     | length (incl. 4B itself) | data              |
/// +----------+--------------------------+-------------------+
/// ```
///
/// Constants for parsing:
/// - `HEADER_CODE_LEN` = 1 byte (message type code, e.g. 'T' = RowDescription)
/// - `HEADER_LEN_SIZE` = 4 bytes (message length, includes itself but NOT the code byte)
/// - `HEADER_TOTAL`   = 5 bytes (minimum bytes needed to read the length field)
pub(super) fn deserialize_cached(cached: Vec<u8>) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut offset = 0;
    let len = cached.len();

    while offset < len {
        // Need at least a full header (code + length) to proceed.
        if offset + HEADER_TOTAL > len {
            debug!(
                "deserializing cached response: not enough bytes for message header (offset={}, len={})",
                offset, len
            );
            break;
        }

        // Read the message length field (4 bytes, big-endian).
        // This length includes the 4-byte length field itself but NOT the code byte.
        let msg_len = u32::from_be_bytes([
            cached[offset + 1],
            cached[offset + 2],
            cached[offset + 3],
            cached[offset + 4],
        ]) as usize;

        // Sanity checks:
        // 1. Length must be at least 4 (the length field itself): if < 4 the data is corrupt.
        // 2. Must not read past the end of the blob.
        if msg_len < 4 || offset + HEADER_CODE_LEN + msg_len > len {
            debug!(
                "deserializing cached response: invalid msg length {} (offset={}, len={})",
                msg_len, offset, len
            );
            break;
        }

        // Full message spans: 1 byte (code) + msg_len (length field + payload)
        let end = offset + HEADER_CODE_LEN + msg_len;

        let msg_bytes: bytes::Bytes = cached[offset..end].to_vec().into();
        if let Ok(msg) = Message::from_bytes(msg_bytes) {
            messages.push(msg);
        }
        offset = end;
    }

    messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::{CommandComplete, Protocol, ReadyForQuery, ToBytes};

    /// Build a raw wire-format blob from a list of typed protocol messages.
    fn wire_bytes(msgs: &[&dyn ToBytes]) -> Vec<u8> {
        let mut buf = Vec::new();
        for msg in msgs {
            buf.extend_from_slice(&msg.to_bytes().unwrap());
        }
        buf
    }

    #[test]
    fn deserialize_empty_input() {
        let messages = deserialize_cached(vec![]);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_single_message() {
        let rfq = ReadyForQuery::idle();
        let blob = wire_bytes(&[&rfq]);
        let messages = deserialize_cached(blob);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].code(), 'Z');
    }

    #[test]
    fn deserialize_multiple_messages_roundtrip() {
        let cc = CommandComplete::new("SELECT 1");
        let rfq = ReadyForQuery::idle();
        let blob = wire_bytes(&[&cc, &rfq]);

        let messages = deserialize_cached(blob);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].code(), 'C');
        assert_eq!(messages[1].code(), 'Z');
    }

    #[test]
    fn deserialize_roundtrip_payload_matches() {
        let cc = CommandComplete::new("SELECT 42");
        let rfq = ReadyForQuery::idle();
        let original: Vec<Message> = vec![
            Message::new(cc.to_bytes().unwrap()),
            Message::new(rfq.to_bytes().unwrap()),
        ];

        // Serialize to flat blob exactly as cache_response does.
        let mut blob = Vec::new();
        for msg in &original {
            blob.extend_from_slice(&msg.to_bytes().unwrap());
        }

        let deserialized = deserialize_cached(blob);
        assert_eq!(deserialized.len(), original.len());
        for (d, o) in deserialized.iter().zip(original.iter()) {
            assert_eq!(d.payload(), o.payload());
        }
    }

    #[test]
    fn deserialize_truncated_header_no_panic() {
        // Only 3 bytes — not enough for a full 5-byte header.
        let truncated = vec![b'Z', 0x00, 0x00];
        let messages = deserialize_cached(truncated);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_truncated_payload_no_panic() {
        // Valid header claiming length 8 (4-byte len field + 4-byte payload),
        // but we only provide the header and 2 payload bytes instead of 4.
        let mut blob = Vec::new();
        blob.push(b'C'); // code byte
        blob.extend_from_slice(&8u32.to_be_bytes()); // length = 8 (includes itself)
        blob.extend_from_slice(&[0u8, 0]); // only 2 of the expected 4 payload bytes
        let messages = deserialize_cached(blob);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_corrupt_length_no_panic() {
        // Length field set to 0 — invalid (must be >= 4).
        let mut blob = Vec::new();
        blob.push(b'Z');
        blob.extend_from_slice(&0u32.to_be_bytes());
        let messages = deserialize_cached(blob);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_length_of_three_no_panic() {
        // Length field = 3 — below minimum of 4, should be rejected.
        let mut blob = Vec::new();
        blob.push(b'Z');
        blob.extend_from_slice(&3u32.to_be_bytes());
        blob.extend_from_slice(&[0u8; 3]);
        let messages = deserialize_cached(blob);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_many_messages() {
        // Round-trip 10 CommandComplete messages.
        let n = 10usize;
        let mut blob = Vec::new();
        for i in 0..n {
            let cc = CommandComplete::new(format!("SELECT {}", i));
            blob.extend_from_slice(&cc.to_bytes().unwrap());
        }

        let messages = deserialize_cached(blob);
        assert_eq!(messages.len(), n);
        for msg in &messages {
            assert_eq!(msg.code(), 'C');
        }
    }
}
