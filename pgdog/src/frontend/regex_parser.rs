use once_cell::sync::Lazy;
use pgdog_config::{General, QueryParserLevel};
use regex::RegexSet;

use crate::frontend::ClientRequest;
use crate::util::truncate_utf8;

/// Whitespace and inline SQL comments (`-- line` and `/* block */`)
/// allowed before a statement keyword.
static COMMENT_PREFIX: &str = r"(?i)^\s*(?:(?:--[^\n]*\n|/\*[\s\S]*?\*/)\s*)*";

static CMD_BASE: &[&str] = &["(RE)?SET", "BEGIN", "COMMIT", "ROLLBACK"];

static CMD_ADVISORY: &[&str] = &[
    r"(?i)\bpg_advisory_lock\b",
    r"(?i)\bpg_advisory_unlock\b",
    r"(?i)\bpg_advisory_unlock_all\b",
    r"(?i)\bpg_advisory_xact_lock\b",
    r"(?i)\bpg_try_advisory_lock\b",
    r"(?i)\bpg_try_advisory_xact_lock\b",
    r"(?i)\bpg_advisory_lock_shared\b",
    r"(?i)\bpg_advisory_unlock_shared\b",
    r"(?i)\bpg_advisory_xact_lock_shared\b",
    r"(?i)\bpg_try_advisory_lock_shared\b",
    r"(?i)\bpg_try_advisory_xact_lock_shared\b",
];

fn cmd_base_patterns() -> impl Iterator<Item = String> {
    CMD_BASE
        .iter()
        .map(|cmd| format!("{}{}", COMMENT_PREFIX, cmd))
}

static CMD_RE: Lazy<RegexSet> = Lazy::new(|| RegexSet::new(cmd_base_patterns()).unwrap());
static CMD_RE_ADVISORY: Lazy<RegexSet> = Lazy::new(|| {
    RegexSet::new(cmd_base_patterns().chain(CMD_ADVISORY.iter().map(|s| s.to_string()))).unwrap()
});

#[derive(Debug, Clone)]
pub(crate) struct RegexParser {
    limit: usize,
    level: QueryParserLevel,
}

impl Default for RegexParser {
    fn default() -> Self {
        Self {
            limit: General::regex_parser_limit(),
            level: QueryParserLevel::default(),
        }
    }
}

impl RegexParser {
    pub(crate) fn new(limit: usize, level: QueryParserLevel) -> Self {
        Self { level, limit }
    }

    /// Check if we should enable the parser just for this request.
    pub(crate) fn use_parser(&self, request: &ClientRequest) -> bool {
        let with_locks = self.level == QueryParserLevel::SessionControlAndLocks;
        let session_control =
            self.level == QueryParserLevel::SessionControl || self.level == QueryParserLevel::Auto;

        if (with_locks || session_control)
            && let Ok(Some(query)) = request.query()
        {
            let prefix = truncate_utf8(query.query(), self.limit);
            if with_locks {
                return CMD_RE_ADVISORY.is_match(prefix);
            } else {
                return CMD_RE.is_match(prefix);
            }
        }

        false
    }
}

#[cfg(test)]
mod test {
    use crate::net::{Parse, ProtocolMessage, Query};

    use super::*;

    fn matches(query: &str) -> bool {
        matches_at(query, QueryParserLevel::SessionControl)
    }

    fn matches_at(query: &str, level: QueryParserLevel) -> bool {
        let mut yes = false;
        for req in [
            ProtocolMessage::from(Query::new(query)),
            Parse::new_anonymous(query).into(),
        ] {
            let req = ClientRequest::from(vec![req]);
            yes = RegexParser::new(General::regex_parser_limit(), level).use_parser(&req);
        }

        yes
    }

    #[test]
    fn test_set() {
        assert!(matches("SET statement_timeout TO 1"));
        assert!(matches("set statement_timeout to 1"));
        assert!(matches("   SET statement_timeout TO 1"));
        assert!(matches("   set statement_timeout to 1"));
    }

    #[test]
    fn test_reset() {
        assert!(matches("RESET statement_timeout"));
        assert!(matches("reset statement_timeout"));
        assert!(matches("   RESET statement_timeout"));
        assert!(matches("RESET ALL"));
    }

    #[test]
    fn test_begin() {
        assert!(matches("BEGIN"));
        assert!(matches("begin"));
        assert!(matches("   BEGIN"));
        assert!(matches("BEGIN WORK REPEATABLE READ"));
        assert!(matches("BEGIN READ ONLY"));
        assert!(matches("begin read only"));
    }

    #[test]
    fn test_commit() {
        assert!(matches("COMMIT"));
        assert!(matches("commit"));
        assert!(matches("   COMMIT"));
    }

    #[test]
    fn test_rollback() {
        assert!(matches("ROLLBACK"));
        assert!(matches("rollback"));
        assert!(matches("   ROLLBACK"));
    }

    #[test]
    fn test_advisory_lock() {
        let l = QueryParserLevel::SessionControlAndLocks;
        assert!(matches_at("SELECT pg_advisory_lock(1)", l));
        assert!(matches_at("select pg_advisory_lock(1, 2)", l));
        assert!(matches_at("SELECT pg_advisory_unlock(1)", l));
        assert!(matches_at("SELECT pg_advisory_unlock_all()", l));
        assert!(matches_at("SELECT pg_advisory_xact_lock(1)", l));
        assert!(matches_at("SELECT pg_try_advisory_lock(1)", l));
        assert!(matches_at("SELECT pg_try_advisory_xact_lock(1)", l));
        assert!(matches_at("SELECT pg_advisory_lock_shared(1)", l));
        assert!(matches_at("SELECT pg_advisory_unlock_shared(1)", l));
        assert!(matches_at("SELECT pg_advisory_xact_lock_shared(1)", l));
        assert!(matches_at("SELECT pg_try_advisory_lock_shared(1)", l));
        assert!(matches_at("SELECT pg_try_advisory_xact_lock_shared(1)", l));
        assert!(matches_at(
            "SELECT pg_try_advisory_lock(? value) FROM (VALUES (?)) (value);",
            l
        ));

        assert!(!matches_at(
            "SELECT pg_advisory_lock(1)",
            QueryParserLevel::SessionControl
        ));
    }

    #[test]
    fn test_scan_prefix_limit() {
        let l = QueryParserLevel::SessionControlAndLocks;
        let long = format!("{}pg_advisory_lock(1)", " ".repeat(1_000));
        assert!(!matches_at(&long, l));

        let short = format!("{}pg_advisory_lock(1)", " ".repeat(900));
        assert!(matches_at(&short, l));
    }

    #[test]
    fn test_limit_longer_than_query() {
        let query = "SET statement_timeout TO 1";

        for limit in [query.len(), query.len() + 1, 10_000, usize::MAX] {
            let req = ClientRequest::from(vec![ProtocolMessage::from(Query::new(query))]);
            let parser = RegexParser::new(limit, QueryParserLevel::SessionControl);
            assert!(parser.use_parser(&req), "limit {} should match", limit);
        }

        // Multi-byte characters at the end don't trip the boundary scan.
        let multibyte = "SET application_name TO 'héllo'";
        let req = ClientRequest::from(vec![ProtocolMessage::from(Query::new(multibyte))]);
        let parser = RegexParser::new(usize::MAX, QueryParserLevel::SessionControl);
        assert!(parser.use_parser(&req));
    }

    #[test]
    fn test_inline_comments() {
        assert!(matches("/* comment */ SET statement_timeout TO 1"));
        assert!(matches("/* comment */SET statement_timeout TO 1"));
        assert!(matches("-- comment\nSET statement_timeout TO 1"));
        assert!(matches("--comment\n  RESET ALL"));
        assert!(matches("/* one */ /* two */ BEGIN"));
        assert!(matches("/* multi\nline\ncomment */ COMMIT"));
        assert!(matches("-- first\n-- second\nROLLBACK"));
        assert!(matches("/* block */ -- line\nbegin read only"));

        // Comment-only or commented-out statements don't match.
        assert!(!matches("-- SET statement_timeout TO 1"));
        assert!(!matches("/* SET statement_timeout TO 1 */ SELECT 1"));
        assert!(!matches("/* comment */ SELECT 1"));
    }

    #[test]
    fn test_leading_whitespace() {
        assert!(matches("   /* comment */ SET statement_timeout TO 1"));
        assert!(matches("\t/* comment */\tBEGIN"));
        assert!(matches("\n\n-- comment\nCOMMIT"));
        assert!(matches(" \t\n SET statement_timeout TO 1"));
        assert!(matches("/* comment */\n\n   ROLLBACK"));
        assert!(matches("  -- one\n  -- two\n  begin"));
    }

    #[test]
    fn test_comment_edge_cases() {
        // Empty and minimal comments.
        assert!(matches("/**/SET statement_timeout TO 1"));
        assert!(matches("/* */BEGIN"));
        assert!(matches("--\nCOMMIT"));

        // Stars and slashes inside block comments.
        assert!(matches("/* a * b */ SET statement_timeout TO 1"));
        assert!(matches("/* a / b ** c */ BEGIN"));

        // Non-greedy: comment ends at the first `*/`.
        assert!(matches("/* a */ SET application_name TO '/* b */'"));

        // Windows line endings in line comments.
        assert!(matches("-- comment\r\nSET statement_timeout TO 1"));

        // Comment-lookalikes inside the comment body.
        assert!(matches("/* -- not a line comment */ ROLLBACK"));
        assert!(matches("-- /* not a block comment\nBEGIN"));

        // Unterminated block comment never reaches a statement.
        assert!(!matches("/* unterminated SET statement_timeout TO 1"));

        // Line comment swallows the rest of the line; next line decides.
        assert!(matches("-- SET commented out\nSET statement_timeout TO 1"));
        assert!(!matches("/* a */ -- SET commented out\nSELECT 1"));
    }

    #[test]
    fn test_advisory_lock_with_comments() {
        let l = QueryParserLevel::SessionControlAndLocks;
        assert!(matches_at("/* comment */ SELECT pg_advisory_lock(1)", l));
        assert!(matches_at("-- comment\nSELECT pg_advisory_unlock(1)", l));
        assert!(matches_at("SELECT /* inline */ pg_try_advisory_lock(1)", l));
    }

    #[test]
    fn test_no_match() {
        assert!(!matches("SELECT 1"));
        assert!(!matches("INSERT INTO users VALUES (1)"));
        assert!(!matches("UPDATE users SET name = 'foo'"));
        assert!(!matches("DELETE FROM users"));
    }
}
