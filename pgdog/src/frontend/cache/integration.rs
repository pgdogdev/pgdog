use std::hash::{Hash, Hasher};

use crate::{
    config::{cache::CachePolicy, config},
    frontend::{
        cache::{
            directive::{self, CacheMode},
            storage::Error as CacheStorageError,
            Cache,
        },
        ClientRequest,
    },
    net::{bind::Bind, FromBytes, Message, Parameters, ToBytes},
};

use tracing::{debug, warn};

pub struct CacheMiss {
    pub key: u64,
    pub ttl: u64,
}

pub enum CacheCheckResult {
    Hit { cached: Vec<u8> },
    Miss(CacheMiss),
    Passthrough,
}

/// Feed `query` into `hasher`, skipping SQL comments and normalising surrounding
/// whitespace — without allocating a `String`.
///
/// Rules applied on-the-fly:
/// - Block comments (`/* … */`, including PostgreSQL nested variants) are treated
///   as a potential token separator: a single space is emitted if needed.
/// - Line comments (`-- …\n`) are treated the same way; the trailing newline
///   becomes the pending separator.
/// - Runs of ASCII whitespace outside string literals are collapsed to a single
///   space and leading/trailing whitespace is suppressed.
/// - String literals (`'…'`, with `''` escapes) are passed through byte-for-byte
///   so that spaces inside them are never removed.
fn hash_query_without_comments<H: Hasher>(query: &str, hasher: &mut H) {
    // pending_space: we *want* to emit a space before the next real token but
    // haven't done so yet (avoids leading space and trailing space).
    let mut pending_space = false;
    // emitted: have we written at least one real byte yet?
    let mut emitted = false;

    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // ---- block comment (supports nested) --------------------------------
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
                        None => break, // malformed: treat as end
                        _ => {}
                    }
                }
                // The comment may stand between two tokens; record the need for
                // a separator but don't emit yet.
                if emitted {
                    pending_space = true;
                }
            }

            // ---- line comment ---------------------------------------------------
            '-' if chars.peek() == Some(&'-') => {
                for ch in chars.by_ref() {
                    if ch == '\n' {
                        break;
                    }
                }
                if emitted {
                    pending_space = true;
                }
            }

            // ---- string literal — pass through verbatim -------------------------
            '\'' => {
                // Flush any pending space before the opening quote.
                if pending_space && emitted {
                    ' '.hash(hasher);
                    pending_space = false;
                }
                c.hash(hasher);
                emitted = true;
                while let Some(ch) = chars.next() {
                    ch.hash(hasher);
                    if ch == '\'' {
                        // Standard SQL escaped quote: two consecutive single-quotes.
                        if chars.peek() == Some(&'\'') {
                            chars.next().unwrap().hash(hasher);
                        } else {
                            break;
                        }
                    }
                }
            }

            // ---- whitespace — collapse to a single pending space ----------------
            c if c.is_ascii_whitespace() => {
                if emitted {
                    pending_space = true;
                }
            }

            // ---- regular character ----------------------------------------------
            c => {
                if pending_space && emitted {
                    ' '.hash(hasher);
                    pending_space = false;
                }
                c.hash(hasher);
                emitted = true;
            }
        }
    }
}

/// Compute the XXH3 cache key hash for a query.
///
/// SQL comments are skipped and surrounding whitespace is normalised on-the-fly
/// while feeding bytes directly into the hasher — no `String` allocation.
pub fn compute_cache_key_hash(database: &str, query: &str, bind: Option<&Bind>) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    database.hash(&mut hasher);
    hash_query_without_comments(query, &mut hasher);
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

        let directive = directive::resolve(client_request, params);
        let cache_config = &config().config.general.cache;

        let ttl = directive.ttl_seconds.unwrap_or(cache_config.ttl);
        let mode = match directive.mode {
            Some(mode) => mode,
            None => match cache_config.policy {
                CachePolicy::NoCache => CacheMode::NoCache,
                CachePolicy::Cache => CacheMode::Cache,
            },
        };

        if mode == CacheMode::NoCache {
            return Ok(CacheCheckResult::Passthrough);
        }

        let key = match directive.key {
            Some(key) => {
                let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
                key.hash(&mut hasher);
                hasher.finish()
            }
            None => {
                let user = params.get_required("user")?;
                let database = params.get_default("database", user);
                let bind = client_request.parameters()?;
                compute_cache_key_hash(database, query.query(), bind)
            }
        };

        if mode == CacheMode::ForceCache {
            return Ok(CacheCheckResult::Miss(CacheMiss { key, ttl }));
        }

        let guard = self.storage.read().await;
        let storage = match guard.as_ref() {
            Some(storage) => storage,
            None => return Ok(CacheCheckResult::Passthrough),
        };
        match storage.get(key).await {
            Ok(cached) => Ok(CacheCheckResult::Hit { cached }),
            Err(CacheStorageError::CacheMiss(_)) => {
                Ok(CacheCheckResult::Miss(CacheMiss { key, ttl }))
            }
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

    pub(super) async fn cache_response(&self, key: u64, messages: Vec<Message>, ttl: u64) {
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

        if let Err(e) = storage.set(key, &buffer, ttl).await {
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
    // hash_query_without_comments tests
    // (verified via compute_cache_key_hash with a fixed database name)
    // -------------------------------------------------------------------------

    /// Helper: hash just the query part (no bind params) with a fixed database.
    fn qhash(query: &str) -> u64 {
        compute_cache_key_hash("db", query, None)
    }

    #[test]
    fn hash_no_comments_same_as_plain() {
        // A query without any comments should hash identically to itself.
        assert_eq!(qhash("SELECT 1"), qhash("SELECT 1"));
    }

    #[test]
    fn hash_block_comment_stripped() {
        // Block comment containing the directive must be invisible to the hash.
        assert_eq!(
            qhash("/* pgdog_cache: cache */ SELECT 1"),
            qhash("SELECT 1"),
        );
    }

    #[test]
    fn hash_line_comment_stripped() {
        assert_eq!(qhash("-- pgdog_cache: cache\nSELECT 1"), qhash("SELECT 1"),);
    }

    #[test]
    fn hash_nested_block_comments_stripped() {
        assert_eq!(
            qhash("/* outer /* inner */ still outer */ SELECT 2"),
            qhash("SELECT 2"),
        );
    }

    #[test]
    fn hash_multiple_block_comments_stripped() {
        assert_eq!(qhash("/* a */ SELECT /* b */ 1"), qhash("SELECT 1"),);
    }

    #[test]
    fn hash_string_literal_contents_preserved() {
        // `/* ... */` inside a string literal must NOT be treated as a comment.
        // The two queries are different so their hashes must differ.
        let h1 = qhash("SELECT '/* not a comment */' FROM t");
        let h2 = qhash("SELECT ' ' FROM t");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_escaped_quotes_in_literal_preserved() {
        // `'it''s fine'` — the embedded `''` must survive; the queries differ.
        let h1 = qhash("SELECT 'it''s fine' FROM t");
        let h2 = qhash("SELECT 'its fine' FROM t");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_whitespace_inside_string_preserved() {
        // Spaces inside string literals must not be collapsed/removed.
        let h1 = qhash("SELECT 'hello world' FROM t");
        let h2 = qhash("SELECT 'helloworld' FROM t");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_inline_comment_no_space_between_tokens() {
        // `SELECT/*c*/1` — comment sits directly between tokens; they must not
        // be merged, so this must equal `SELECT 1` not `SELECT1`.
        assert_eq!(qhash("SELECT/*c*/1"), qhash("SELECT 1"));
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
