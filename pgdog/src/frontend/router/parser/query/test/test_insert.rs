use std::ops::Deref;

use crate::frontend::router::parser::{Error, Shard};
use crate::net::messages::bind::Parameter;

use super::setup::*;

#[test]
fn test_insert_numeric() {
    let mut test = QueryParserTest::new();

    let command =
        test.execute(vec![Query::new(
        "INSERT INTO sharded (id, sample_numeric) VALUES (2, -987654321.123456789::NUMERIC)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_negative_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded (id) VALUES (-5)").into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_with_cast_on_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded (id, value) VALUES (42::BIGINT, 'test')").into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_multi_row() {
    let mut test = QueryParserTest::new();

    // Multi-row inserts go to all shards (split by query engine later)
    let command = test.execute(vec![
        Parse::named(
            "__test_multi",
            "INSERT INTO sharded (id, value) VALUES ($1, 'a'), ($2, 'b')",
        )
        .into(),
        Bind::new_params(
            "__test_multi",
            &[Parameter::new(b"0"), Parameter::new(b"2")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::All));
}

#[test]
fn test_insert_select() {
    // Without subquery rewrite being on, we don't know the sharding keys, so we must error (as opposed to incorrectly routing)
    for sql in [
        "INSERT INTO sharded (id, value) SELECT id, value FROM other_table WHERE id = 1",
        "INSERT INTO sharded (id) SELECT g FROM generate_series(1, 10) g",
        "INSERT INTO sharded (id) SELECT generate_series(1, 3)",
        "INSERT INTO sharded (id, value) SELECT * FROM json_to_recordset('[]') AS t(id bigint, value text)",
        "INSERT INTO sharded (id, value) SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, value)",
        "INSERT INTO sharded (id, value) SELECT 1, 'a' UNION ALL SELECT 2, 'b'",
        "INSERT INTO sharded (id, value) SELECT * FROM unnest(ARRAY[1, 2], ARRAY['a', 'b'])",
        "INSERT INTO sharded SELECT * FROM other_table",
    ] {
        let mut test = QueryParserTest::new();
        let error = test.try_execute(vec![Query::new(sql).into()]).unwrap_err();

        assert!(
            matches!(error, Error::InsertSelectWithoutShardingKey),
            "{sql}: {error:?}"
        );
    }

    // Without the sharding key column specified, we assume that the default for the column generates the keys on the shard that will perform the INSERT, thus, round-robin is
    // acceptable and stays as-is (no error)
    for sql in [
        "INSERT INTO sharded (value) SELECT v FROM generate_series(1, 10) v",
        "INSERT INTO sharded (value) SELECT 'a'",
    ] {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(sql).into()]);
        assert!(command.route().shard().is_direct(), "{sql}");
    }

    // Allow round-robin here too since we don't have the Bind param yet
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::new_anonymous(
            "INSERT INTO sharded (id, value) SELECT id, value FROM sharded WHERE id = $1",
        )
        .into(),
        Describe::new_statement("").into(),
        Sync.into(),
    ]);
    assert!(command.route().is_write());
}

#[test]
fn test_insert_default_values() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded DEFAULT VALUES").into(),
    ]);

    assert!(command.route().is_write());
    assert!(command.route().shard().is_direct());
}

// Check that the router doesn't return the INSERT .. SELECT error for the unnest(..) INSERTs that go to split insert,
// and that it still returns the error when the rewrite doesn't split the INSERT (e.g., with LIMIT).
#[test]
fn test_insert_select_from_unnest() {
    let mut config = crate::config::config().deref().clone();
    config.config.rewrite.split_inserts = pgdog_config::RewriteMode::Rewrite;

    let unnest_insert = |sql: &str| -> Vec<crate::net::ProtocolMessage> {
        vec![
            Parse::new_anonymous(sql).into(),
            Bind::new_params("", &[Parameter::new(b"{1,2}"), Parameter::new(b"{a,b}")]).into(),
            Execute::new().into(),
            Sync.into(),
        ]
    };

    // Rewrite splits the rows of the INSERT by shard (thus, no error)
    let mut test = QueryParserTest::new_with_config(&config);
    let command = test.execute(unnest_insert(
        "INSERT INTO sharded (id, value) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text)",
    ));
    assert!(command.route().is_write());

    // With LIMIT, the rewrite doesn't split the INSERT, router returns error
    let mut test = QueryParserTest::new_with_config(&config);
    let error = test
        .try_execute(unnest_insert(
            "INSERT INTO sharded (id, value) SELECT * FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text) LIMIT 1",
        ))
        .unwrap_err();
    assert!(matches!(error, Error::InsertSelectWithoutShardingKey));

    // Without the sharding key column, rewrite doesn't split the INSERT, column default generates the keys on round-robin shard
    let mut test = QueryParserTest::new_with_config(&config);
    let command = test.execute(vec![
        Parse::new_anonymous(
            "INSERT INTO sharded (value) SELECT * FROM unnest($1::pg_catalog._text)",
        )
        .into(),
        Bind::new_params("", &[Parameter::new(b"{a,b}")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);
    assert!(command.route().shard().is_direct());
}

#[test]
fn test_insert_select_with_subquery_rewrite() {
    let mut config = crate::config::config().deref().clone();
    config.config.rewrite.subqueries = pgdog_config::RewriteMode::Rewrite;
    crate::config::set(config.clone()).unwrap();

    // subquery rewrite evaluates the SELECT before routing (thus, router doesn't need to error)
    let mut test = QueryParserTest::new_with_config(&config);
    let command = test.execute(vec![
        Query::new("INSERT INTO sharded (id, value) SELECT g, 'a' FROM generate_series(1, 10) g")
            .into(),
    ]);
    assert!(command.route().is_write());

    // subquery rewrite gap still errors (as UNION is not handled yet)
    let mut test = QueryParserTest::new_with_config(&config);
    let error = test
        .try_execute(vec![
            Query::new("INSERT INTO sharded (id, value) SELECT 1, 'a' UNION ALL SELECT 2, 'b'")
                .into(),
        ])
        .unwrap_err();
    assert!(matches!(error, Error::InsertSelectWithoutShardingKey));
}
