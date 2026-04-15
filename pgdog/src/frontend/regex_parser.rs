use once_cell::sync::Lazy;
use pgdog_config::QueryParserLevel;
use regex::RegexSet;

use crate::{frontend::ClientRequest, net::ProtocolMessage};

static CMD_BASE: &[&str] = &[
    "(?i)^ *(RE)?SET",
    "(?i)^ *BEGIN",
    "(?i)^ *COMMIT",
    "(?i)^ *ROLLBACK",
];

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

static CMD_RE: Lazy<RegexSet> = Lazy::new(|| RegexSet::new(CMD_BASE).unwrap());
static CMD_RE_ADVISORY: Lazy<RegexSet> =
    Lazy::new(|| RegexSet::new(CMD_BASE.iter().chain(CMD_ADVISORY.iter())).unwrap());

fn scan_prefix(query: &str, limit: usize) -> &str {
    if query.len() <= limit {
        query
    } else {
        let mut end = limit;
        while !query.is_char_boundary(end) {
            end -= 1;
        }
        &query[..end]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RegexParser {
    limit: usize,
    level: QueryParserLevel,
}

impl Default for RegexParser {
    fn default() -> Self {
        Self {
            limit: 256,
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

        if with_locks || session_control {
            for message in request.iter() {
                if let ProtocolMessage::Query(query) = message {
                    let prefix = scan_prefix(query.query(), self.limit);
                    if with_locks {
                        return CMD_RE_ADVISORY.is_match(prefix);
                    } else {
                        return CMD_RE.is_match(prefix);
                    }
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod test {
    use crate::net::Query;

    use super::*;

    fn matches(query: &str) -> bool {
        matches_at(query, QueryParserLevel::SessionControl)
    }

    fn matches_at(query: &str, level: QueryParserLevel) -> bool {
        let req = ClientRequest::from(vec![Query::new(query).into()]);
        RegexParser::new(1_000, level).use_parser(&req)
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
    fn test_no_match() {
        assert!(!matches("SELECT 1"));
        assert!(!matches("INSERT INTO users VALUES (1)"));
        assert!(!matches("UPDATE users SET name = 'foo'"));
        assert!(!matches("DELETE FROM users"));
    }
}
