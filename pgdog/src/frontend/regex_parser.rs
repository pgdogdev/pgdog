use once_cell::sync::Lazy;
use regex::RegexSet;

use crate::{frontend::ClientRequest, net::ProtocolMessage};

static CMD_RE: Lazy<RegexSet> = Lazy::new(|| {
    RegexSet::new([
        "(?i)^ *(RE)?SET",
        "(?i)^ *BEGIN",
        "(?i)^ *COMMIT",
        "(?i)^ *ROLLBACK",
    ])
    .unwrap()
});

pub(crate) struct RegexParser {}

impl RegexParser {
    /// Check if we should enable the parser just for this request.
    pub(crate) fn use_parser(request: &ClientRequest) -> bool {
        for message in request.iter() {
            if let ProtocolMessage::Query(query) = message {
                return CMD_RE.is_match(query.query());
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
        let req = ClientRequest::from(vec![Query::new(query).into()]);
        RegexParser::use_parser(&req)
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
    fn test_no_match() {
        assert!(!matches("SELECT 1"));
        assert!(!matches("INSERT INTO users VALUES (1)"));
        assert!(!matches("UPDATE users SET name = 'foo'"));
        assert!(!matches("DELETE FROM users"));
    }
}
