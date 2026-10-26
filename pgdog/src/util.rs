//! What's a project without a util module.

pub(crate) mod retry;
pub(crate) mod server;
pub(crate) mod sql;
pub(crate) mod stats;
pub(crate) mod sync;
pub(crate) mod system;
pub(crate) mod time;

pub(crate) use time::{
    format_time, human_duration, human_duration_display, millis, postgres_now, safe_interval,
    safe_sleep, safe_timeout,
};

use futures::future::Either;
use once_cell::sync::Lazy;
use rand::{Rng, distr::Alphanumeric};
use std::ops::ControlFlow;
use std::{env, future::Future, future::ready};
use tokio::task::{JoinError, spawn_blocking};

use crate::net::Parameters; // 0.8

/// Compare two byte slices in constant time with respect to their contents.
///
/// The running time depends only on the input lengths, never on the byte
/// values, so it cannot leak (via a timing side channel) how many leading
/// bytes matched. Use this wherever an attacker-supplied value is compared
/// against a secret (passwords, cancel keys); a short-circuiting `==`/`memcmp`
/// there is a covert timing channel that lets an attacker recover the secret
/// byte by byte (cf. PostgreSQL CVE-2026-6478, the MD5 password comparison).
///
/// Length is not treated as secret: a length mismatch returns `false` early.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    aws_lc_rs::constant_time::verify_slices_are_equal(a, b).is_ok()
}

/// Generate a random string of length n.
pub(crate) fn random_string(n: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

// Generate a unique 8-character hex instance ID on first access
static INSTANCE_ID: Lazy<String> = Lazy::new(|| {
    if let Ok(node_id) = env::var("NODE_ID") {
        node_id
    } else {
        let mut rng = rand::rng();
        (0..8)
            .map(|_| {
                let n: u8 = rng.random_range(0..16);
                format!("{:x}", n)
            })
            .collect()
    }
});

/// Get the instance ID for this pgdog instance.
/// This is generated once at startup and persists for the lifetime of the process.
pub(crate) fn instance_id() -> &'static str {
    &INSTANCE_ID
}

/// Get an externally assigned, unique, node identifier
/// for this instance of PgDog.
///
/// Requires the `NODE_ID` environment variable. Accepts a bare number,
/// or the documented `<prefix>-<number>` form.
///
/// Returns `None` when `NODE_ID` is unset, or when it does not end in a
/// number.
pub(crate) fn node_id() -> Option<u64> {
    let node_id = env::var("NODE_ID").ok()?;
    let digits = node_id
        .rsplit_once('-')
        .map_or(node_id.as_str(), |(_, digits)| digits);

    digits.parse().ok()
}

static DEPLOYMENT_ID: Lazy<Option<String>> = Lazy::new(|| env::var("DEPLOYMENT_ID").ok());

/// Get the ID of this PgDog deployment.
///
/// This should be _globally_ unique
/// and is used to differentiate 2pc transactions.
///
pub(crate) fn deployment_id() -> Option<&'static str> {
    DEPLOYMENT_ID.as_deref()
}

static HOSTNAME: Lazy<String> = Lazy::new(|| {
    let hostname = env::var("HOSTNAME").unwrap_or_default();
    let host = env::var("HOST").unwrap_or_default();
    if hostname.is_empty() { host } else { hostname }
});

pub(crate) fn hostname() -> &'static str {
    &HOSTNAME
}

/// Escape PostgreSQL identifiers by doubling any embedded quotes.
pub(crate) fn escape_identifier(s: &str) -> String {
    s.replace("\"", "\"\"")
}

/// Get PgDog's version string.
pub(crate) fn pgdog_version() -> String {
    format!(
        "v{} [main@{}, pgdog-plugin {}, {}]",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH"),
        pgdog_plugin::VERSION,
        pgdog_plugin::RUSTC_VERSION,
    )
}

/// Format a number with commas for readability.
/// Example: 1234567 -> "1,234,567"
pub(crate) fn number_human(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format a byte count into a human-readable string.
pub(crate) fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes < TB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    }
}

/// Get user and database parameters.
///
/// These parameters are standard and defined by the Postgres protocol.
///
/// # Arguments
///
/// - `params`: Client parameters extracted from the [`crate::net::Startup`] message.
///
/// # Return
///
/// Tuple of (user, database).
///
pub(crate) fn user_database_from_params(params: &Parameters) -> (&str, &str) {
    let user = params.get_default("user", "postgres");
    let database = params.get_default("database", user);

    (user, database)
}

/// Truncate `s` to at most `limit` bytes, rounding down to the nearest UTF-8
/// character boundary so the result is always valid UTF-8.
pub(crate) fn truncate_utf8(s: &str, limit: usize) -> &str {
    &s[..s.floor_char_boundary(limit)]
}

/// Sanitize a query sample for one-line log output: truncate to at most
/// `limit` bytes on a UTF-8 character boundary and replace control
/// characters (including newlines) with spaces so attacker-controlled
/// bytes can't forge or flood log lines.
pub(crate) fn sanitize_log_sample(s: &str, limit: usize) -> String {
    truncate_utf8(s, limit).replace(|c: char| c.is_control(), " ")
}

/// Run on the blocking pool when background workers are enabled, or inline otherwise.
pub(crate) fn maybe_spawn_blocking<F, R>(f: F) -> impl Future<Output = Result<R, JoinError>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let background_workers = crate::config::config_quick()
        .config
        .general
        .background_workers;

    if background_workers > 0 {
        Either::Left(spawn_blocking(f))
    } else {
        Either::Right(ready(Ok(f())))
    }
}

pub(crate) trait ResultControlFlowExt<T, E> {
    fn break_err<B>(self) -> ControlFlow<Result<B, E>, T>;
}

impl<T, E> ResultControlFlowExt<T, E> for Result<T, E> {
    fn break_err<B>(self) -> ControlFlow<Result<B, E>, T> {
        match self {
            Ok(t) => ControlFlow::Continue(t),
            Err(e) => ControlFlow::Break(Err(e)),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("simple"), "simple");
        assert_eq!(escape_identifier("has\"quote"), "has\"\"quote");
        assert_eq!(escape_identifier("\"starts_with"), "\"\"starts_with");
        assert_eq!(escape_identifier("ends_with\""), "ends_with\"\"");
        assert_eq!(
            escape_identifier("\"multiple\"quotes\""),
            "\"\"multiple\"\"quotes\"\""
        );
    }

    #[test]
    fn test_instance_id_format() {
        let _guard = remove_env_var("NODE_ID");
        let id = instance_id();
        assert_eq!(id.len(), 8);
        // All characters should be valid hex digits (0-9, a-f)
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        // All alphabetic characters should be lowercase
        assert!(
            id.chars()
                .filter(|c| c.is_alphabetic())
                .all(|c| c.is_lowercase())
        );
    }

    #[test]
    fn test_instance_id_consistency() {
        let id1 = instance_id();
        let id2 = instance_id();
        assert_eq!(id1, id2); // Should be the same for lifetime of process
    }

    #[test]
    fn test_node_id_unset() {
        let _guard = remove_env_var("NODE_ID");
        assert_eq!(node_id(), None);
    }

    #[test]
    fn test_node_id_not_a_number() {
        let _guard = set_env_var("NODE_ID", "pgdog-abc");
        assert_eq!(node_id(), None);
    }

    #[test]
    fn test_node_id_bare_number() {
        let _guard = set_env_var("NODE_ID", "7");
        assert_eq!(node_id(), Some(7));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1572864), "1.50 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1610612736), "1.50 GB");
        assert_eq!(format_bytes(1099511627776), "1.00 TB");
    }

    #[test]
    fn test_number_human() {
        assert_eq!(number_human(0), "0");
        assert_eq!(number_human(1), "1");
        assert_eq!(number_human(12), "12");
        assert_eq!(number_human(123), "123");
        assert_eq!(number_human(1234), "1,234");
        assert_eq!(number_human(12345), "12,345");
        assert_eq!(number_human(123456), "123,456");
        assert_eq!(number_human(1234567), "1,234,567");
        assert_eq!(number_human(1234567890), "1,234,567,890");
    }

    #[test]
    fn test_node_id_set() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        assert_eq!(node_id(), Some(1));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hunter2", b"hunter2"));
        assert!(!constant_time_eq(b"hunter2", b"hunter3"));
        // Different lengths must not match.
        assert!(!constant_time_eq(b"hunter2", b"hunter22"));
        assert!(!constant_time_eq(b"", b"x"));
        // Two empty slices are equal.
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_truncate_utf8_ascii() {
        assert_eq!(truncate_utf8("SELECT 1", 4096), "SELECT 1"); // under limit
        assert_eq!(truncate_utf8("SELECT 1", 6), "SELECT"); // truncated
    }

    #[test]
    fn test_truncate_utf8_multibyte() {
        // Mix of 2-byte (é), 3-byte (€), and 4-byte (𝄞) characters.
        let s = "é€𝄞é€𝄞"; // 2+3+4+2+3+4 = 18 bytes

        // Every possible byte limit produces valid UTF-8.
        for limit in 0..=s.len() {
            assert!(std::str::from_utf8(truncate_utf8(s, limit).as_bytes()).is_ok());
        }

        assert_eq!(truncate_utf8(s, 0), ""); // empty
        assert_eq!(truncate_utf8(s, 2), "é"); // exact: end of é
        assert_eq!(truncate_utf8(s, 3), "é"); // 1 byte into € → walk back
        assert_eq!(truncate_utf8(s, 5), "é€"); // exact: end of €
        assert_eq!(truncate_utf8(s, 6), "é€"); // 1 byte into 𝄞 → walk back
        assert_eq!(truncate_utf8(s, 9), "é€𝄞"); // exact: end of 𝄞
    }

    #[test]
    fn test_sanitize_log_sample() {
        // Truncates to the limit on a char boundary.
        assert_eq!(sanitize_log_sample("SELECT 1", 6), "SELECT");

        // Newlines, carriage returns, tabs, and escape sequences become spaces.
        assert_eq!(
            sanitize_log_sample("SELECT\r\n1\t--\x1b[31mforged\x1b[0m", 4096),
            "SELECT  1 -- [31mforged [0m"
        );

        // Truncation happens before sanitization: limit applies to input bytes.
        assert_eq!(sanitize_log_sample("a\nb\nc", 3), "a b");
    }
}
