use std::hash::{Hash, Hasher};

use crate::net::bind::Bind;

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

#[cfg(test)]
mod tests {
    use super::*;

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
