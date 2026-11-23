use super::*;
use crate::backend::ShardingSchema;
use crate::frontend::router::parser::StatementParser;
use crate::net::messages::{Bind, bind::Parameter};

fn locks(query: &str) -> Vec<AdvisoryLock> {
    locks_with_bind(query, None)
}

fn locks_with_bind(query: &str, bind: Option<&Bind>) -> Vec<AdvisoryLock> {
    let mut v: Vec<_> = extracted(query, bind).iter().copied().collect();
    v.sort_by_key(|l| {
        let id = match l.id {
            Some(AdvisoryLockId::OneParameter(id)) => Some((id, None)),
            Some(AdvisoryLockId::TwoParameters(a, b)) => Some((a as i64, Some(b))),
            None => None,
        };
        (id, l.unlock, l.try_lock, l.column_index, l.row_index)
    });
    v
}

fn extracted(query: &str, bind: Option<&Bind>) -> AdvisoryLocks {
    let schema = ShardingSchema::default();
    let raw = pg_raw_parse::parse(query).expect("valid SQL");
    let stmt = raw.stmts().next().expect("one statement");
    let mut parser = StatementParser::new(stmt, bind.map(Into::into), &schema);
    parser.extract_advisory_locks()
}

#[test]
fn lock_result_columns_preserve_duplicate_calls() {
    let bind = Bind::new_params("", &[Parameter::new(b"42")]);
    let locks = extracted(
        "SELECT 123, pg_try_advisory_lock($1) AS acquired, \
         pg_try_advisory_lock($1), pg_advisory_lock(42), \
         pg_advisory_lock(42), pg_catalog.pg_try_advisory_xact_lock_shared(1, 2), \
         pg_advisory_unlock(42), pg_advisory_unlock_all()",
        Some(&bind),
    );
    let mut locks: Vec<_> = locks.iter().copied().collect();
    locks.sort_by_key(|lock| lock.column_index);
    assert_eq!(locks.len(), 7);
    for (index, lock) in locks.iter().enumerate() {
        assert_eq!(lock.column_index, Some(index + 1));
    }
    assert_eq!(locks[0].id, Some(AdvisoryLockId::OneParameter(42)));
    assert!(locks[0].try_lock);
    assert!(locks[1].try_lock);
    assert!(!locks[2].try_lock);
    assert!(!locks[3].try_lock);
    assert_eq!(locks[4].id, Some(AdvisoryLockId::TwoParameters(1, 2)));
    assert_eq!(locks[4].scope, LockScope::Transaction);
    assert!(locks[4].try_lock);
    assert!(locks[5].unlock);
    assert!(locks[6].unlock_all);
}

#[test]
fn lock_result_columns_exclude_indirect_outputs() {
    for query in [
        "SELECT NOT pg_try_advisory_lock(42)",
        "SELECT CASE WHEN pg_try_advisory_lock(42) THEN true ELSE false END",
        "SELECT coalesce(pg_try_advisory_lock(42), false)",
        "SELECT pg_try_advisory_lock(42)::text",
        "SELECT 1 WHERE pg_try_advisory_lock(42)",
        "SELECT (SELECT pg_try_advisory_lock(42))",
        "WITH c AS (SELECT pg_try_advisory_lock(42)) SELECT * FROM c",
        "SELECT pg_try_advisory_lock(42) UNION ALL SELECT false",
        "INSERT INTO t SELECT pg_try_advisory_lock(42)",
        "SELECT (SELECT pg_advisory_lock(42))",
    ] {
        let locks = extracted(query, None);
        assert_eq!(locks.iter().count(), 1, "{query}");
        assert!(
            locks.iter().all(|lock| lock.column_index.is_none()),
            "{query}"
        );
    }
}

#[test]
fn lock_result_columns_survive_nested_queries() {
    let locks = extracted(
        "WITH c AS (SELECT pg_try_advisory_lock(10)) \
         SELECT (SELECT pg_try_advisory_lock(20)), \
         pg_advisory_lock(42) AS acquired, \
         (SELECT pg_try_advisory_lock(30)) \
         FROM c WHERE pg_try_advisory_lock(40)",
        None,
    );
    assert_eq!(locks.iter().count(), 5);
    for lock in locks.iter() {
        assert_eq!(
            lock.column_index,
            (lock.id == Some(AdvisoryLockId::OneParameter(42))).then_some(1),
        );
    }
}

#[test]
fn lock_result_columns_stop_at_star_expansion() {
    for star in ["*", "t.*", "(t).*", "(t.composite).*"] {
        let query = format!("SELECT pg_advisory_lock(42), {star}, pg_try_advisory_lock(43) FROM t");
        let locks = extracted(&query, None);
        assert_eq!(locks.iter().count(), 2, "{query}");
        for lock in locks.iter() {
            assert_eq!(
                lock.column_index,
                (lock.id == Some(AdvisoryLockId::OneParameter(42))).then_some(0),
                "{query}"
            );
        }
    }
    assert!(
        extracted("SELECT count(*), pg_advisory_lock(42)", None)
            .iter()
            .all(|lock| lock.column_index == Some(1)),
    );
}

#[test]
fn lock_result_columns_with_unknown_or_varying_keys() {
    for function in ["pg_advisory_lock", "pg_try_advisory_lock"] {
        let unresolved = extracted(&format!("SELECT {function}((SELECT 42))"), None);
        assert_eq!(unresolved.iter().count(), 1);
        assert!(
            unresolved
                .iter()
                .all(|lock| { lock.column_index == Some(0) && lock.id.is_none() })
        );
        let expanded = locks(&format!(
            "SELECT {function}(value) FROM (VALUES (1), (2)) AS t(value)"
        ));
        assert_eq!(expanded.len(), 2);
        assert_eq!(expanded[0].id, Some(AdvisoryLockId::OneParameter(1)));
        assert_eq!(expanded[1].id, Some(AdvisoryLockId::OneParameter(2)));
        assert_eq!(expanded[0].row_index, 0);
        assert_eq!(expanded[1].row_index, 1);
        assert!(expanded.iter().all(|lock| lock.column_index == Some(0)));
    }
}

fn at_row(mut lock: AdvisoryLock, row_index: usize) -> AdvisoryLock {
    lock.row_index = row_index;
    lock
}

fn session(id: Option<AdvisoryLockId>, unlock: bool) -> AdvisoryLock {
    AdvisoryLock {
        id,
        unlock,
        unlock_all: false,
        try_lock: false,
        column_index: Some(0),
        row_index: 0,
        scope: LockScope::Session,
    }
}

fn xact(id: Option<AdvisoryLockId>, unlock: bool) -> AdvisoryLock {
    AdvisoryLock {
        id,
        unlock,
        unlock_all: false,
        try_lock: false,
        column_index: Some(0),
        row_index: 0,
        scope: LockScope::Transaction,
    }
}

fn unlock_all() -> AdvisoryLock {
    AdvisoryLock {
        id: None,
        unlock: true,
        unlock_all: true,
        try_lock: false,
        column_index: Some(0),
        row_index: 0,
        scope: LockScope::Session,
    }
}

#[test]
fn unresolved_unlock_is_distinct_from_unlock_all() {
    let null_bind = Bind::new_params("", &[Parameter::new_null()]);
    for query in [
        "SELECT pg_advisory_unlock(NULL::bigint)",
        "SELECT pg_advisory_unlock($1::bigint)",
        "SELECT pg_advisory_unlock(1, NULL::integer)",
        "SELECT pg_advisory_unlock((SELECT 42))",
        "SELECT pg_advisory_unlock(value) FROM (VALUES (NULL::bigint)) AS t(value)",
    ] {
        assert_eq!(
            locks_with_bind(query, Some(&null_bind)),
            vec![session(None, true)],
            "{query}"
        );
    }
    assert_eq!(locks("SELECT pg_advisory_unlock_all()"), vec![unlock_all()]);
}

#[test]
fn lock_and_unlock() {
    assert_eq!(
        locks("SELECT pg_advisory_lock(42)"),
        vec![session(Some(AdvisoryLockId::OneParameter(42)), false)],
    );
    assert_eq!(
        locks("SELECT pg_advisory_unlock(42)"),
        vec![session(Some(AdvisoryLockId::OneParameter(42)), true)],
    );
}

#[test]
fn lock_with_two_param() {
    assert_eq!(
        locks("SELECT pg_advisory_lock(1, 2)"),
        vec![session(Some(AdvisoryLockId::TwoParameters(1, 2)), false)]
    );
}

#[test]
fn lock_with_hashtext_both() {
    // Try out hashtext and hashtextended; compared against the numbers Postgres outputs!
    assert_eq!(
        locks("SELECT pg_advisory_lock(hashtext('hello world'))"),
        vec![session(
            Some(AdvisoryLockId::OneParameter(1021725223)),
            false
        )]
    );

    assert_eq!(
        locks("SELECT pg_advisory_lock(hashtextextended('hello world', 123))"),
        vec![session(
            Some(AdvisoryLockId::OneParameter(3896024775453578562)),
            false
        )]
    );
}

#[test]
fn pg_catalog_qualified_advisory_calls() {
    let bind = Bind::new_params("", &[Parameter::new(b"123")]);
    for function in [
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_try_advisory_lock",
        "pg_try_advisory_lock_shared",
        "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_xact_lock_shared",
        "pg_advisory_unlock",
    ] {
        for argument in ["123", "$1::bigint"] {
            let unqualified = format!("SELECT {function}({argument})");
            let expected = locks_with_bind(&unqualified, Some(&bind));
            assert!(!expected.is_empty(), "{unqualified}");
            for schema in ["pg_catalog", "\"pg_catalog\""] {
                let qualified = format!("SELECT {schema}.{function}({argument})");
                assert_eq!(
                    locks_with_bind(&qualified, Some(&bind)),
                    expected,
                    "{qualified}"
                );
            }
            let custom = format!("SELECT other.{function}({argument})");
            assert!(locks_with_bind(&custom, Some(&bind)).is_empty(), "{custom}");
        }
    }
    assert_eq!(
        locks("SELECT pg_catalog.pg_advisory_unlock_all()"),
        vec![unlock_all()],
    );
}

#[test]
fn bigint_argument() {
    // Values larger than i32 are encoded as Float in PG internally.
    assert_eq!(
        locks("SELECT pg_advisory_lock(9000000000)"),
        vec![session(
            Some(AdvisoryLockId::OneParameter(9_000_000_000)),
            false
        )],
    );
}

#[test]
fn all_session_lock_variants() {
    for (q, try_lock) in [
        ("SELECT pg_advisory_lock(7)", false),
        ("SELECT pg_try_advisory_lock(7)", true),
        ("SELECT pg_advisory_lock_shared(7)", false),
        ("SELECT pg_try_advisory_lock_shared(7)", true),
    ] {
        assert_eq!(
            locks(q),
            vec![AdvisoryLock {
                try_lock,
                ..session(Some(AdvisoryLockId::OneParameter(7)), false)
            }],
            "{q}"
        );
    }
}

#[test]
fn xact_variants_have_transaction_scope() {
    // xact locks must still pin the backend for the lifetime of the transaction,
    // but the engine drops them at COMMIT/ROLLBACK.
    for (q, try_lock) in [
        ("SELECT pg_advisory_xact_lock(7)", false),
        ("SELECT pg_advisory_xact_lock_shared(7)", false),
        ("SELECT pg_try_advisory_xact_lock(7)", true),
        ("SELECT pg_try_advisory_xact_lock_shared(7)", true),
    ] {
        assert_eq!(
            locks(q),
            vec![AdvisoryLock {
                try_lock,
                ..xact(Some(AdvisoryLockId::OneParameter(7)), false)
            }],
            "{q}"
        );
    }
}

#[test]
fn multiple_calls_keep_distinct_columns() {
    assert_eq!(
        locks(
            "SELECT pg_advisory_lock(5), pg_advisory_lock(5), pg_advisory_lock(6), \
             pg_try_advisory_lock(5), pg_try_advisory_lock(5)",
        ),
        vec![
            session(Some(AdvisoryLockId::OneParameter(5)), false),
            AdvisoryLock {
                column_index: Some(1),
                ..session(Some(AdvisoryLockId::OneParameter(5)), false)
            },
            AdvisoryLock {
                try_lock: true,
                column_index: Some(3),
                ..session(Some(AdvisoryLockId::OneParameter(5)), false)
            },
            AdvisoryLock {
                try_lock: true,
                column_index: Some(4),
                ..session(Some(AdvisoryLockId::OneParameter(5)), false)
            },
            AdvisoryLock {
                column_index: Some(2),
                ..session(Some(AdvisoryLockId::OneParameter(6)), false)
            },
        ],
    );
}

#[test]
fn cast_and_cte() {
    assert_eq!(
        locks("SELECT pg_try_advisory_lock(9)::bool"),
        vec![AdvisoryLock {
            try_lock: true,
            column_index: None,
            ..session(Some(AdvisoryLockId::OneParameter(9)), false)
        }],
    );
    assert_eq!(
        locks("WITH x AS (SELECT pg_advisory_lock(11)) SELECT * FROM x"),
        vec![AdvisoryLock {
            column_index: None,
            ..session(Some(AdvisoryLockId::OneParameter(11)), false)
        }],
    );
}

#[test]
fn param_without_bind_is_ignored() {
    // Without a Bind message, a parameter placeholder means the prepared
    // statement is only being parsed — no lock is actually taken.
    assert!(locks("SELECT pg_advisory_lock($1)").is_empty());
}

#[test]
fn two_params_without_bind_is_ignored() {
    assert!(locks("SELECT pg_advisory_lock($1, $2)").is_empty());
    assert!(locks("SELECT pg_advisory_lock($1, 1)").is_empty());
    assert!(locks("SELECT pg_adivsory_lock(1, $2)").is_empty());
}

#[test]
fn unlock_all_without_bind() {
    // unlock_all takes no arguments, so it always applies.
    assert_eq!(locks("SELECT pg_advisory_unlock_all()"), vec![unlock_all()],);
}

#[test]
fn ignored_cases() {
    // Schema-qualified — not the builtin.
    assert!(locks("SELECT other.pg_advisory_lock(1)").is_empty());
    // Unrelated functions.
    assert!(locks("SELECT 1, now()").is_empty());
}

#[test]
fn key_from_values_subquery_no_bind() {
    // Without a Bind, parameter-based VALUES rows are skipped — the
    // prepared statement is only being parsed, no lock is taken.
    assert!(locks("SELECT pg_advisory_lock(value) FROM (VALUES ($1)) AS t(value)").is_empty());
    assert!(locks("SELECT pg_advisory_unlock(value) FROM (VALUES ($1)) AS t(value)").is_empty());
    assert!(locks("SELECT pg_try_advisory_lock(value) FROM (VALUES ($1)) AS t(value)").is_empty());
}

#[test]
fn xact_lock_with_param_no_bind() {
    // Without a Bind the prepared statement is just being parsed.
    assert!(locks("SELECT pg_advisory_xact_lock($1)").is_empty());
}

#[test]
fn param_resolved_from_bind() {
    let bind = Bind::new_params("", &[Parameter::new(b"4242")]);
    assert_eq!(
        locks_with_bind("SELECT pg_advisory_lock($1)", Some(&bind)),
        vec![session(Some(AdvisoryLockId::OneParameter(4242)), false)],
    );
    assert_eq!(
        locks_with_bind("SELECT pg_advisory_xact_lock($1)", Some(&bind)),
        vec![xact(Some(AdvisoryLockId::OneParameter(4242)), false)],
    );
    assert_eq!(
        locks_with_bind("SELECT pg_advisory_unlock($1)", Some(&bind)),
        vec![session(Some(AdvisoryLockId::OneParameter(4242)), true)],
    );
}

#[test]
fn bind_bigint_value() {
    // Keys wider than i32 are encoded as text on the wire but still
    // decode cleanly through FromDataType<i64>.
    let bind = Bind::new_params("", &[Parameter::new(b"9000000000")]);
    assert_eq!(
        locks_with_bind("SELECT pg_advisory_lock($1)", Some(&bind)),
        vec![session(
            Some(AdvisoryLockId::OneParameter(9_000_000_000)),
            false
        )],
    );
}

#[test]
fn multiple_locks_in_one_query_with_bind() {
    // Single query taking multiple advisory locks from distinct bind params.
    let bind = Bind::new_params(
        "",
        &[
            Parameter::new(b"11"),
            Parameter::new(b"22"),
            Parameter::new(b"33"),
        ],
    );
    assert_eq!(
        locks_with_bind(
            "SELECT pg_advisory_lock($1), pg_advisory_xact_lock($2), pg_advisory_unlock($3)",
            Some(&bind),
        ),
        vec![
            session(Some(AdvisoryLockId::OneParameter(11)), false),
            AdvisoryLock {
                column_index: Some(1),
                ..xact(Some(AdvisoryLockId::OneParameter(22)), false)
            },
            AdvisoryLock {
                column_index: Some(2),
                ..session(Some(AdvisoryLockId::OneParameter(33)), true)
            },
        ],
    );
}

#[test]
fn multiple_literal_locks_in_one_query() {
    assert_eq!(
        locks(
            "SELECT pg_advisory_lock(10), pg_advisory_xact_lock(20), \
             pg_advisory_unlock(30), pg_advisory_unlock_all()",
        ),
        vec![
            AdvisoryLock {
                column_index: Some(3),
                ..unlock_all()
            },
            session(Some(AdvisoryLockId::OneParameter(10)), false),
            AdvisoryLock {
                column_index: Some(1),
                ..xact(Some(AdvisoryLockId::OneParameter(20)), false)
            },
            AdvisoryLock {
                column_index: Some(2),
                ..session(Some(AdvisoryLockId::OneParameter(30)), true)
            },
        ],
    );
}

#[test]
fn values_multiple_rows_expand_to_multiple_locks() {
    // `pg_advisory_lock(value) FROM (VALUES (1),(2),(3)) AS t(value)` is
    // called once per row, so the parser should emit one lock per row.
    assert_eq!(
        locks("SELECT pg_advisory_lock(value) FROM (VALUES (10), (20), (30)) AS t(value)",),
        vec![
            session(Some(AdvisoryLockId::OneParameter(10)), false),
            at_row(session(Some(AdvisoryLockId::OneParameter(20)), false), 1),
            at_row(session(Some(AdvisoryLockId::OneParameter(30)), false), 2),
        ],
    );
}

#[test]
fn advisory_lock_from_values_without_explicit_column_name() {
    assert_eq!(
        locks("SELECT pg_advisory_lock(column1) FROM (VALUES (10), (20), (30))",),
        vec![
            session(Some(AdvisoryLockId::OneParameter(10)), false),
            at_row(session(Some(AdvisoryLockId::OneParameter(20)), false), 1),
            at_row(session(Some(AdvisoryLockId::OneParameter(30)), false), 2),
        ],
    );
}

#[test]
fn advisory_lock_when_client_is_sadistic() {
    assert_eq!(
        locks(
            "SELECT pg_advisory_lock(column1), (SELECT pg_advisory_lock(c) FROM (VALUES (20), (30)) AS t(c)) FROM (VALUES (10))",
        ),
        vec![
            session(Some(AdvisoryLockId::OneParameter(10)), false),
            AdvisoryLock {
                column_index: None,
                ..session(Some(AdvisoryLockId::OneParameter(20)), false)
            },
            AdvisoryLock {
                column_index: None,
                row_index: 1,
                ..session(Some(AdvisoryLockId::OneParameter(30)), false)
            },
        ],
    );
}

#[test]
fn values_multiple_rows_with_bind() {
    let bind = Bind::new_params(
        "",
        &[
            Parameter::new(b"41"),
            Parameter::new(b"42"),
            Parameter::new(b"43"),
        ],
    );
    assert_eq!(
        locks_with_bind(
            "SELECT pg_advisory_lock(value) FROM (VALUES ($1), ($2), ($3)) AS t(value)",
            Some(&bind),
        ),
        vec![
            session(Some(AdvisoryLockId::OneParameter(41)), false),
            at_row(session(Some(AdvisoryLockId::OneParameter(42)), false), 1),
            at_row(session(Some(AdvisoryLockId::OneParameter(43)), false), 2),
        ],
    );

    for function in ["pg_advisory_lock", "pg_try_advisory_lock"] {
        let query = format!(
            "SELECT {function}(value), {function}(value) \
             FROM (VALUES (42), ($1), (42)) AS t(value)"
        );
        let unbound = extracted(&query, None);
        let mut positions: Vec<_> = unbound
            .iter()
            .map(|lock| (lock.column_index, lock.row_index))
            .collect();
        positions.sort();
        assert_eq!(
            positions,
            vec![(Some(0), 0), (Some(0), 2), (Some(1), 0), (Some(1), 2)],
        );

        let bind = Bind::new_params("", &[Parameter::new(b"42")]);
        let bound = extracted(&query, Some(&bind));
        let mut positions: Vec<_> = bound
            .iter()
            .map(|lock| (lock.column_index, lock.row_index))
            .collect();
        positions.sort();
        assert_eq!(
            positions,
            vec![
                (Some(0), 0),
                (Some(0), 1),
                (Some(0), 2),
                (Some(1), 0),
                (Some(1), 1),
                (Some(1), 2),
            ],
        );
    }
}

#[test]
fn values_multi_rows_unlock_and_xact() {
    // Same multi-row expansion for unlock and xact variants.
    assert_eq!(
        locks("SELECT pg_advisory_unlock(value) FROM (VALUES (1), (2)) AS t(value)",),
        vec![
            session(Some(AdvisoryLockId::OneParameter(1)), true),
            at_row(session(Some(AdvisoryLockId::OneParameter(2)), true), 1)
        ],
    );
    assert_eq!(
        locks("SELECT pg_advisory_xact_lock(value) FROM (VALUES (5), (6)) AS t(value)",),
        vec![
            xact(Some(AdvisoryLockId::OneParameter(5)), false),
            at_row(xact(Some(AdvisoryLockId::OneParameter(6)), false), 1)
        ],
    );
}

#[test]
fn param_out_of_range_fallback() {
    // $2 has no bound value — we should still record the lock but leave id=None.
    let bind = Bind::new_params("", &[Parameter::new(b"99")]);
    assert_eq!(
        locks_with_bind(
            "SELECT pg_advisory_lock($1), pg_advisory_lock($2)",
            Some(&bind),
        ),
        vec![
            AdvisoryLock {
                column_index: Some(1),
                ..session(None, false)
            },
            session(Some(AdvisoryLockId::OneParameter(99)), false)
        ],
    );
}
