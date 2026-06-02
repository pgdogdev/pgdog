use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use crate::{
    frontend::{
        cache::{storage::Error as CacheStorageError, CacheDecision},
        ClientRequest,
    },
    net::{bind::Bind, FromBytes, Message, Parameters, ToBytes},
};

use tracing::{debug, warn};

use super::{policy, Cache};

pub struct CacheMiss {
    pub cache_key_hash: u64,
    pub ttl: u64,
}

pub enum CacheCheckResult {
    Hit { cached: Vec<u8> },
    Miss(CacheMiss),
    Passthrough,
}

/// Strip SQL block comments (`/* ... */`, including nested) and line comments (`-- ...`)
/// from `query`, preserving string literals (`'...'`).
///
/// Returns `Cow::Borrowed(query)` without any allocation when no comment markers
/// are found.  Only allocates and builds a new `String` when a `/*` or `--`
/// sequence is actually present in the input.
pub fn strip_sql_comments(query: &str) -> Cow<'_, str> {
    // Fast path: scan bytes for comment markers before doing any allocation.
    let bytes = query.as_bytes();
    let has_comment = bytes.windows(2).any(|w| w == b"/*" || w == b"--");
    if !has_comment {
        return Cow::Borrowed(query);
    }

    let mut result = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // Block comment — supports PostgreSQL nested `/* */`.
            '/' if chars.peek() == Some(&'*') => {
                chars.next(); // consume '*'
                let mut depth = 1u32;
                while depth > 0 {
                    match chars.next() {
                        Some('/') if chars.peek() == Some(&'*') => {
                            chars.next();
                            depth += 1;
                        }
                        Some('*') if chars.peek() == Some(&'/') => {
                            chars.next();
                            depth -= 1;
                        }
                        None => break, // malformed input
                        _ => {}
                    }
                }
                // Replace the entire comment with a single space to avoid
                // accidentally merging adjacent tokens (e.g. `SELECT/*c*/1`).
                result.push(' ');
            }
            // Line comment.
            '-' if chars.peek() == Some(&'-') => {
                for ch in chars.by_ref() {
                    if ch == '\n' {
                        result.push('\n');
                        break;
                    }
                }
            }
            // String literal — pass through unchanged so we don't mistake `--`
            // or `/*` inside a string for a comment.
            '\'' => {
                result.push(c);
                while let Some(ch) = chars.next() {
                    result.push(ch);
                    if ch == '\'' {
                        // Standard SQL escaped quote: two consecutive single-quotes.
                        if chars.peek() == Some(&'\'') {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            _ => result.push(c),
        }
    }

    Cow::Owned(result)
}

/// Compute the XXH3 cache key hash for a query.
///
/// All SQL comments are stripped from `query` before hashing so the hash is identical
/// regardless of whether the cache directive was supplied via a comment or a connection
/// parameter.
pub fn compute_cache_key_hash(database: &str, query: &str, bind: Option<&Bind>) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    database.hash(&mut hasher);
    let stripped = strip_sql_comments(query);
    stripped.trim().hash(&mut hasher);
    if let Some(bind) = bind {
        for param in bind.params_raw() {
            param.len.hash(&mut hasher);
            param.data.hash(&mut hasher);
        }
    }
    hasher.finish()
}

const HEADER_CODE_LEN: usize = 1;
const HEADER_LEN_SIZE: usize = 4;
const HEADER_TOTAL: usize = HEADER_CODE_LEN + HEADER_LEN_SIZE;

impl Cache {
    pub(super) async fn cache_check(
        &self,
        in_transaction: bool,
        client_request: &ClientRequest,
        params: &Parameters,
    ) -> Result<CacheCheckResult, crate::frontend::Error> {
        if in_transaction || !client_request.is_executable() {
            return Ok(CacheCheckResult::Passthrough);
        }

        let route = match client_request.route.as_ref() {
            Some(r) => r,
            None => return Ok(CacheCheckResult::Passthrough),
        };

        // Detect read-only status via the AST parser's route classification.
        // When caching is enabled, the query parser is auto-enabled.
        let is_read = route.is_read();
        if !is_read {
            return Ok(CacheCheckResult::Passthrough);
        }

        let query = match client_request.query() {
            Ok(Some(q)) => q,
            _ => return Ok(CacheCheckResult::Passthrough),
        };

        let user = params.get_required("user")?;
        let database = params.get_default("database", user);
        let bind = client_request.parameters()?;

        let decision = policy::resolve(client_request, params, is_read).await;
        let ttl = match decision {
            CacheDecision::Skip => return Ok(CacheCheckResult::Passthrough),
            CacheDecision::ForceCache(ttl) => {
                return Ok(CacheCheckResult::Miss(CacheMiss {
                    cache_key_hash: compute_cache_key_hash(database, query.query(), bind),
                    ttl,
                }))
            }
            CacheDecision::Cache(ttl) => ttl,
        };

        let cache_key_hash = compute_cache_key_hash(database, query.query(), bind);
        let guard = self.storage.read().await;
        let storage = match guard.as_ref() {
            Some(storage) => storage,
            None => return Ok(CacheCheckResult::Passthrough),
        };
        match storage.get(cache_key_hash).await {
            Ok(cached) => Ok(CacheCheckResult::Hit { cached }),
            Err(CacheStorageError::CacheMiss(_)) => Ok(CacheCheckResult::Miss(CacheMiss {
                cache_key_hash,
                ttl,
            })),
            Err(e) => {
                warn!("{}", e);
                Ok(CacheCheckResult::Passthrough)
            }
        }
    }

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

    pub(super) async fn cache_response(
        &self,
        cache_key_hash: u64,
        messages: Vec<Message>,
        ttl: u64,
    ) {
        let guard = self.storage.read().await;
        let storage = match guard.as_ref() {
            Some(s) if s.is_enabled() => s,
            _ => return,
        };

        if messages.is_empty() {
            return;
        }

        let mut buffer = Vec::new();
        for msg in &messages {
            match msg.to_bytes() {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => {
                    warn!("Failed to serialize message for caching: {}", e);
                    return;
                }
            }
        }

        if buffer.is_empty() {
            return;
        }

        if let Err(e) = storage.set(cache_key_hash, &buffer, ttl).await {
            warn!("{}", e);
        }
    }
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
        let messages = Cache::deserialize_cached(vec![]);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_single_message() {
        let rfq = ReadyForQuery::idle();
        let blob = wire_bytes(&[&rfq]);
        let messages = Cache::deserialize_cached(blob);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].code(), 'Z');
    }

    #[test]
    fn deserialize_multiple_messages_roundtrip() {
        let cc = CommandComplete::new("SELECT 1");
        let rfq = ReadyForQuery::idle();
        let blob = wire_bytes(&[&cc, &rfq]);

        let messages = Cache::deserialize_cached(blob);
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

        let deserialized = Cache::deserialize_cached(blob);
        assert_eq!(deserialized.len(), original.len());
        for (d, o) in deserialized.iter().zip(original.iter()) {
            assert_eq!(d.payload(), o.payload());
        }
    }

    #[test]
    fn deserialize_truncated_header_no_panic() {
        // Only 3 bytes — not enough for a full 5-byte header.
        let truncated = vec![b'Z', 0x00, 0x00];
        let messages = Cache::deserialize_cached(truncated);
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
        let messages = Cache::deserialize_cached(blob);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_corrupt_length_no_panic() {
        // Length field set to 0 — invalid (must be >= 4).
        let mut blob = Vec::new();
        blob.push(b'Z');
        blob.extend_from_slice(&0u32.to_be_bytes());
        let messages = Cache::deserialize_cached(blob);
        assert!(messages.is_empty());
    }

    #[test]
    fn deserialize_length_of_three_no_panic() {
        // Length field = 3 — below minimum of 4, should be rejected.
        let mut blob = Vec::new();
        blob.push(b'Z');
        blob.extend_from_slice(&3u32.to_be_bytes());
        blob.extend_from_slice(&[0u8; 3]);
        let messages = Cache::deserialize_cached(blob);
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

        let messages = Cache::deserialize_cached(blob);
        assert_eq!(messages.len(), n);
        for msg in &messages {
            assert_eq!(msg.code(), 'C');
        }
    }

    // -------------------------------------------------------------------------
    // strip_sql_comments tests
    // -------------------------------------------------------------------------

    #[test]
    fn strip_no_comments() {
        let q = "SELECT 1";
        assert_eq!(strip_sql_comments(q), "SELECT 1");
    }

    #[test]
    fn strip_no_comments_returns_borrowed() {
        // When there are no comment markers the original slice must be returned
        // without any allocation (Cow::Borrowed).
        let q = "SELECT 1 FROM t WHERE id = 42";
        assert!(matches!(
            strip_sql_comments(q),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn strip_with_comment_returns_owned() {
        let q = "/* hint */ SELECT 1";
        assert!(matches!(strip_sql_comments(q), std::borrow::Cow::Owned(_)));
    }

    #[test]
    fn strip_block_comment() {
        let q = "/* pgdog_cache: cache */ SELECT 1";
        let stripped = strip_sql_comments(q);
        assert!(!stripped.contains("pgdog_cache"));
        assert!(stripped.contains("SELECT 1"));
    }

    #[test]
    fn strip_line_comment() {
        let q = "-- pgdog_cache: cache\nSELECT 1";
        let stripped = strip_sql_comments(q);
        assert!(!stripped.contains("pgdog_cache"));
        assert!(stripped.contains("SELECT 1"));
    }

    #[test]
    fn strip_nested_block_comments() {
        let q = "/* outer /* inner */ still outer */ SELECT 2";
        let stripped = strip_sql_comments(q);
        assert!(!stripped.contains("outer"));
        assert!(!stripped.contains("inner"));
        assert!(stripped.contains("SELECT 2"));
    }

    #[test]
    fn strip_does_not_remove_string_literal_contents() {
        let q = "SELECT '/* not a comment */' FROM t";
        let stripped = strip_sql_comments(q);
        // The string literal must be preserved verbatim.
        assert!(stripped.contains("'/* not a comment */'"));
    }

    #[test]
    fn strip_preserves_escaped_quotes_in_literal() {
        let q = "SELECT 'it''s fine' FROM t";
        let stripped = strip_sql_comments(q);
        assert_eq!(stripped, "SELECT 'it''s fine' FROM t");
    }

    #[test]
    fn strip_multiple_block_comments() {
        let q = "/* a */ SELECT /* b */ 1";
        let stripped = strip_sql_comments(q);
        assert!(!stripped.contains("/* a */"));
        assert!(!stripped.contains("/* b */"));
        assert!(stripped.contains("SELECT"));
        assert!(stripped.contains("1"));
    }

    // -------------------------------------------------------------------------
    // compute_cache_key_hash tests
    // -------------------------------------------------------------------------

    #[test]
    fn hash_is_stable() {
        let h1 = compute_cache_key_hash("mydb", "SELECT 1", None);
        let h2 = compute_cache_key_hash("mydb", "SELECT 1", None);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_differs_by_database() {
        let h1 = compute_cache_key_hash("db1", "SELECT 1", None);
        let h2 = compute_cache_key_hash("db2", "SELECT 1", None);
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_differs_by_query() {
        let h1 = compute_cache_key_hash("db", "SELECT 1", None);
        let h2 = compute_cache_key_hash("db", "SELECT 2", None);
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_same_with_and_without_cache_comment() {
        // A block comment containing the cache directive must be stripped so
        // the hash is the same whether the directive was in a comment or a
        // connection parameter.
        let h_with_comment =
            compute_cache_key_hash("db", "/* pgdog_cache: cache */ SELECT 1", None);
        let h_without_comment = compute_cache_key_hash("db", "SELECT 1", None);
        assert_eq!(h_with_comment, h_without_comment);
    }

    #[test]
    fn hash_same_for_force_cache_and_regular_comment() {
        // force_cache and cache hints should produce the same hash (both are
        // stripped before hashing, so the underlying query is identical).
        let h_force = compute_cache_key_hash("db", "/* pgdog_cache: force_cache */ SELECT 1", None);
        let h_cache = compute_cache_key_hash("db", "/* pgdog_cache: cache */ SELECT 1", None);
        let h_plain = compute_cache_key_hash("db", "SELECT 1", None);
        assert_eq!(h_force, h_cache);
        assert_eq!(h_force, h_plain);
    }

    #[test]
    fn hash_same_for_line_comment_cache_directive() {
        let h_with_line = compute_cache_key_hash("db", "-- pgdog_cache: cache\nSELECT 1", None);
        let h_plain = compute_cache_key_hash("db", "SELECT 1", None);
        assert_eq!(h_with_line, h_plain);
    }

    #[test]
    fn hash_differs_by_bind_params() {
        use crate::net::messages::bind::{Bind, Parameter};
        use bytes::Bytes;
        use pgdog_postgres_types::Format;

        let make_bind = |val: &'static [u8]| {
            let mut b = Bind::default();
            b.push_param(
                Parameter {
                    len: val.len() as i32,
                    data: Bytes::from_static(val),
                },
                Format::Text,
            );
            b
        };

        let b1 = make_bind(b"1");
        let b2 = make_bind(b"2");
        let h1 = compute_cache_key_hash("db", "SELECT $1", Some(&b1));
        let h2 = compute_cache_key_hash("db", "SELECT $1", Some(&b2));
        assert_ne!(h1, h2);
    }
}
