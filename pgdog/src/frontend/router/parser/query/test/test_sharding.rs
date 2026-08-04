use std::collections::HashSet;
use std::ops::Deref;

use crate::config::config;
use crate::frontend::Command;
use crate::frontend::router::parser::{Cache, Shard};

use super::setup::{QueryParserTest, *};

use pgdog_config::ShardedTableConfig;

#[test]
fn test_show_shards() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW pgdog.shards").into()]);

    assert!(matches!(command, Command::InternalField { .. }));
}

#[test]
fn test_close_direct_single_shard() {
    let mut test = QueryParserTest::new_single_shard(&config());

    let command = test.execute(vec![Close::named("test").into(), Sync.into()]);

    match command {
        Command::Query(route) => assert_eq!(route.shard(), &Shard::Direct(0), "{:?}", route),
        _ => panic!("expected Query, got {command:?}"),
    }
}

#[test]
fn test_dry_run_simple() {
    let mut test = QueryParserTest::new_single_shard(&config()).with_dry_run();

    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 1234 */ SELECT * FROM sharded").into(),
    ]);

    let cache = Cache::queries();
    let stmt = cache.values().next().unwrap();
    assert_eq!(stmt.stats.lock().direct, 1);
    assert_eq!(stmt.stats.lock().multi, 0);
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_omni_round_robin() {
    let mut omni_round_robin = HashSet::new();
    let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = 1";

    for _ in 0..10 {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(q).into()]);

        if let Command::Query(query) = command {
            assert!(matches!(query.shard(), Shard::Direct(_)));
            omni_round_robin.insert(query.shard().clone());
        }
    }

    assert_eq!(omni_round_robin.len(), 2);
}

#[test]
fn test_omni_sticky() {
    let mut omni_sticky = HashSet::new();
    let q =
        "SELECT sharded_omni_sticky.* FROM sharded_omni_sticky WHERE sharded_omni_sticky.id = $1";

    // Use a single test instance with consistent Sticky across all iterations
    let mut test = QueryParserTest::new();
    for _ in 0..10 {
        let command = test.execute(vec![Query::new(q).into()]);

        if let Command::Query(query) = command {
            assert!(matches!(query.shard(), Shard::Direct(_)));
            omni_sticky.insert(query.shard().clone());
        }
    }

    assert_eq!(omni_sticky.len(), 1);
}

#[test]
fn test_omni_sharded_table_takes_priority() {
    let q = "
        SELECT
            sharded_omni.*,
            sharded.*
        FROM
            sharded_omni
        INNER JOIN
            sharded
        ON sharded_omni.id = sharded.i
    WHERE sharded.id = 5";

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(q).into()]);
    let first_shard = command.route().shard().clone();

    for _ in 0..5 {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(q).into()]);
        assert_eq!(&first_shard, command.route().shard());
        assert!(matches!(first_shard, Shard::Direct(_)));
    }
}

#[test]
fn test_omni_all_tables_must_be_omnisharded() {
    let q = "SELECT * FROM sharded_omni INNER JOIN not_sharded ON sharded_omni.id = not_sharded.id INNER JOIN sharded ON sharded.id = sharded_omni.id WHERE sharded_omni = $1";

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(q).into()]);

    assert!(matches!(command.route().shard(), Shard::All));
}

#[test]
fn test_omni_flag_set_for_select() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded_omni WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_update() {
    let mut test = QueryParserTest::new();
    let q = "UPDATE sharded_omni SET value = 'test' WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_delete() {
    let mut test = QueryParserTest::new();
    let q = "DELETE FROM sharded_omni WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_insert() {
    let mut test = QueryParserTest::new();
    let q = "INSERT INTO sharded_omni (id, value) VALUES (1, 'test')";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_unconfigured_update() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Query::new("UPDATE not_sharded SET value = 'test'").into(),
    ]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_unconfigured_delete() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new("DELETE FROM not_sharded").into()]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_set_for_unconfigured_insert() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Query::new("INSERT INTO not_sharded VALUES (1)").into(),
    ]);
    assert!(command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_not_set_for_regular_sharded() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded WHERE id = 1";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(!command.route().is_omnisharded());
}

#[test]
fn test_omni_flag_not_set_for_regular_sharded_dml() {
    for query in [
        "INSERT INTO sharded (id) VALUES (1)",
        "UPDATE sharded SET value = 'test' WHERE id = 1",
        "DELETE FROM sharded WHERE id = 1",
    ] {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(!command.route().is_omnisharded(), "query: {query}");
    }
}

#[test]
fn test_omni_flag_not_set_for_schema_sharded_dml() {
    for query in [
        "INSERT INTO shard_0.not_sharded VALUES (1)",
        "UPDATE shard_0.not_sharded SET value = 'test'",
        "DELETE FROM shard_0.not_sharded",
    ] {
        let mut test = QueryParserTest::new();
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(!command.route().is_omnisharded(), "query: {query}");
    }
}

#[test]
fn test_omni_flag_not_set_when_joined_with_sharded() {
    let mut test = QueryParserTest::new();
    let q = "SELECT * FROM sharded_omni INNER JOIN sharded ON sharded_omni.id = sharded.id WHERE sharded.id = 5";
    let command = test.execute(vec![Query::new(q).into()]);
    assert!(!command.route().is_omnisharded());
}

/// Test that omnisharded config overrides sharded table config.
/// When a table is in both sharded_tables AND omnisharded config,
/// the omnisharded config should take priority.
///
/// Note: Cluster::new_test() hardcodes omnisharded tables (sharded_omni, sharded_omni_sticky)
/// so we test by adding "sharded_omni" to sharded_tables and verifying omnisharded wins.
#[test]
fn test_omnisharded_overrides_sharded_table_config() {
    // Add "sharded_omni" (which is already in omnisharded config in Cluster::new_test)
    // to sharded_tables config - omnisharded should still take priority
    let mut config_with_both = config().deref().clone();
    config_with_both
        .config
        .sharded_tables
        .push(ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded_omni".into()),
            column: "id".into(),
            ..Default::default()
        });

    // Query against "sharded_omni" which is now in BOTH sharded_tables AND omnisharded
    // Should be treated as omnisharded (round-robin), NOT sharded (deterministic)
    let q = "SELECT * FROM sharded_omni WHERE id = 1";

    // Run multiple times to verify round-robin behavior (omnisharded)
    let mut shards_seen = HashSet::new();
    for _ in 0..10 {
        let mut test = QueryParserTest::new_with_config(&config_with_both);
        let command = test.execute(vec![Query::new(q).into()]);
        match command {
            Command::Query(route) => {
                assert!(
                    route.is_omnisharded(),
                    "Query against table in both configs should have omni flag (omnisharded wins)"
                );
                shards_seen.insert(route.shard().clone());
            }
            _ => panic!("Expected Query command"),
        }
    }

    // Should see multiple shards due to round-robin (omnisharded behavior)
    // If sharded config won, we'd see only one shard (deterministic)
    assert!(
        shards_seen.len() > 1,
        "Omnisharded should override sharded config, using round-robin. Saw {:?}",
        shards_seen
    );
}

fn lookup_rule_tables() -> crate::backend::ShardedTables {
    use crate::backend::ShardedTables;
    use crate::frontend::router::sharding::ShardedTable;
    use pgdog_config::{DataType, OmnishardedTable, SystemCatalogsBehavior};

    ShardedTables::new(
        vec![ShardedTable {
            database: "pgdog".into(),
            name: None,
            column: "organization_id".into(),
            data_type: DataType::Varchar,
            lookup_query: Some(
                "SELECT COALESCE(parent_organization_id, id) FROM organizations WHERE id = $1"
                    .into(),
            ),
            ..Default::default()
        }],
        vec![OmnishardedTable {
            name: "organizations".into(),
            sticky_routing: false,
        }],
        false,
        SystemCatalogsBehavior::default(),
    )
}

fn warm_lookup_cache(tables: &crate::backend::ShardedTables, value: &str, translated: &str) {
    use crate::frontend::router::sharding::LookupTable;

    tables.lookup_cache().insert(
        LookupTable {
            schema: None,
            name: None,
            column: "organization_id".into(),
        },
        value.into(),
        translated.into(),
    );
}

/// A sharding key comment on a statement routed by it goes through
/// the lookup: a cache miss makes the route carry the pending lookup
/// instead of routing by the untranslated value.
#[test]
fn test_comment_key_misses_lookup_records_pending() {
    let tables = lookup_rule_tables();
    let mut test = QueryParserTest::new().with_sharded_tables(tables);

    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT * FROM some_table").into(),
    ]);

    let pending = command.route().pending_lookups();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].value, "org_child");
}

/// Same statement with the translation cached routes by the
/// translated value.
#[test]
fn test_comment_key_translates_through_lookup() {
    use crate::frontend::router::sharding::shard_value;
    use pgdog_config::DataType;

    let tables = lookup_rule_tables();
    warm_lookup_cache(&tables, "org_child", "org_root");
    let mut test = QueryParserTest::new().with_sharded_tables(tables);

    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT * FROM some_table").into(),
    ]);

    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(
        command.route().shard(),
        &shard_value("org_root", &DataType::Varchar, 2, &vec![], 0)
    );
}

/// SET pgdog.sharding_key goes through the lookup too.
#[test]
fn test_set_sharding_key_goes_through_lookup() {
    use crate::frontend::router::sharding::shard_value;
    use pgdog_config::DataType;

    // Cold cache: the route carries the pending lookup.
    let tables = lookup_rule_tables();
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables.clone())
        .without_sharded_schemas()
        .with_param("pgdog.sharding_key", "org_child");
    let command = test.execute(vec![Query::new("SELECT * FROM some_table").into()]);
    let pending = command.route().pending_lookups();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].value, "org_child");

    // Warm cache: routes by the translated value.
    warm_lookup_cache(&tables, "org_child", "org_root");
    let command = test.execute(vec![Query::new("SELECT * FROM some_table").into()]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(
        command.route().shard(),
        &shard_value("org_root", &DataType::Varchar, 2, &vec![], 0)
    );
}

/// A write that only touches omnisharded tables must reach every
/// shard: a shard directive on it is an error, with a cold cache
/// (the key's lookup doesn't run) and with a warm one.
#[test]
fn test_comment_key_errors_on_omnisharded_write() {
    use crate::frontend::router::parser::Error;

    // Cold cache.
    let tables = lookup_rule_tables();
    let mut test = QueryParserTest::new().with_sharded_tables(tables.clone());
    let result = test.try_execute(vec![
        Query::new(
            "/* pgdog_sharding_key: 'org_child' */ INSERT INTO organizations (id, name) VALUES ('org_child', 'child')",
        )
        .into(),
    ]);
    assert!(matches!(result, Err(Error::OmniWriteWithDirective)));

    // Warm cache.
    warm_lookup_cache(&tables, "org_child", "org_root");
    let result = test.try_execute(vec![
        Query::new(
            "/* pgdog_sharding_key: 'org_child' */ INSERT INTO organizations (id, name) VALUES ('org_child', 'child')",
        )
        .into(),
    ]);
    assert!(matches!(result, Err(Error::OmniWriteWithDirective)));

    // An explicit shard directive errors the same way.
    let result = test.try_execute(vec![
        Query::new(
            "/* pgdog_shard: 1 */ INSERT INTO organizations (id, name) VALUES ('org_child', 'child')",
        )
        .into(),
    ]);
    assert!(matches!(result, Err(Error::OmniWriteWithDirective)));

    // Without the directive, the write broadcasts.
    let command = test.execute(vec![
        Query::new("INSERT INTO organizations (id, name) VALUES ('org_child', 'child')").into(),
    ]);
    assert!(command.route().is_omnisharded());
    assert_eq!(command.route().shard(), &Shard::All);

    // Reads keep the directive: any shard holds the data.
    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT * FROM organizations").into(),
    ]);
    assert!(command.route().is_omnisharded());
    assert!(command.route().shard().is_direct());
}

/// SET pgdog.sharding_key errors on omnisharded writes the same way.
#[test]
fn test_set_key_errors_on_omnisharded_write() {
    use crate::frontend::router::parser::Error;

    let tables = lookup_rule_tables();
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables)
        .without_sharded_schemas()
        .with_param("pgdog.sharding_key", "org_child");
    let result = test.try_execute(vec![
        Query::new("INSERT INTO organizations (id, name) VALUES ('org_child', 'child')").into(),
    ]);
    assert!(matches!(result, Err(Error::OmniWriteWithDirective)));
}

/// Translations resolved for a statement route it without consulting
/// the lookup cache: the second routing pass after resolving lookups
/// can't miss, even if the cache already evicted the entry.
#[test]
fn test_resolved_lookups_route_without_cache() {
    use crate::frontend::router::sharding::{LookupTable, ResolvedLookups, shard_value};
    use pgdog_config::DataType;

    let key = LookupTable {
        schema: None,
        name: None,
        column: "organization_id".into(),
    };
    let mut resolved = ResolvedLookups::default();
    resolved.insert(key, "org_child".into(), "org_root".into());

    // The cache is fresh and empty: only the resolved translations
    // can route these.
    let mut test = QueryParserTest::new()
        .with_sharded_tables(lookup_rule_tables())
        .without_sharded_schemas()
        .with_resolved_lookups(resolved);
    let expected = shard_value("org_root", &DataType::Varchar, 2, &vec![], 0);

    // Statement-extracted key.
    let command = test.execute(vec![
        Query::new("SELECT * FROM some_table WHERE organization_id = 'org_child'").into(),
    ]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(command.route().shard(), &expected);

    // Comment directive: parsed before routing with a cold cache, so
    // its pending lookup gets a second chance at routing time.
    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT 1").into(),
    ]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(command.route().shard(), &expected);
}

/// SET pgdog.sharding_key routes from resolved translations too.
#[test]
fn test_resolved_lookups_route_set_key() {
    use crate::frontend::router::sharding::{LookupTable, ResolvedLookups, shard_value};
    use pgdog_config::DataType;

    let key = LookupTable {
        schema: None,
        name: None,
        column: "organization_id".into(),
    };
    let mut resolved = ResolvedLookups::default();
    resolved.insert(key, "org_child".into(), "org_root".into());

    let mut test = QueryParserTest::new()
        .with_sharded_tables(lookup_rule_tables())
        .without_sharded_schemas()
        .with_param("pgdog.sharding_key", "org_child")
        .with_resolved_lookups(resolved);

    let command = test.execute(vec![Query::new("SELECT * FROM some_table").into()]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(
        command.route().shard(),
        &shard_value("org_root", &DataType::Varchar, 2, &vec![], 0)
    );
}

fn shard_mode_tables() -> crate::backend::ShardedTables {
    use crate::backend::ShardedTables;
    use crate::frontend::router::sharding::ShardedTable;
    use pgdog_config::{DataType, LookupResult, OmnishardedTable, SystemCatalogsBehavior};

    ShardedTables::new(
        vec![ShardedTable {
            database: "pgdog".into(),
            name: None,
            column: "organization_id".into(),
            data_type: DataType::Varchar,
            lookup_query: Some("SELECT shard_id::text FROM organizations WHERE id = $1".into()),
            lookup_result: LookupResult::Shard,
            ..Default::default()
        }],
        vec![OmnishardedTable {
            name: "organizations".into(),
            sticky_routing: false,
        }],
        false,
        SystemCatalogsBehavior::default(),
    )
}

/// Shard-mode lookups route by the returned shard number, never
/// hashing it. Warm cache: statement, comment directive and SET.
#[test]
fn test_lookup_result_shard_routes_directly() {
    let tables = shard_mode_tables();
    warm_lookup_cache(&tables, "org_child", "1");
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables.clone())
        .without_sharded_schemas();

    // Statement-extracted key.
    let command = test.execute(vec![
        Query::new("SELECT * FROM some_table WHERE organization_id = 'org_child'").into(),
    ]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(command.route().shard(), &Shard::Direct(1));

    // Comment directive.
    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT 1").into(),
    ]);
    assert_eq!(command.route().shard(), &Shard::Direct(1));

    // SET pgdog.sharding_key.
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables)
        .without_sharded_schemas()
        .with_param("pgdog.sharding_key", "org_child");
    let command = test.execute(vec![Query::new("SELECT * FROM some_table").into()]);
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}

/// A shard number out of range or non-numeric fails the statement.
#[test]
fn test_lookup_result_shard_out_of_range_errors() {
    // The test cluster has 2 shards.
    let tables = shard_mode_tables();
    warm_lookup_cache(&tables, "org_child", "5");
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables)
        .without_sharded_schemas();
    let result = test.try_execute(vec![
        Query::new("SELECT * FROM some_table WHERE organization_id = 'org_child'").into(),
    ]);
    assert!(result.is_err());

    let tables = shard_mode_tables();
    warm_lookup_cache(&tables, "org_child", "not-a-shard");
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables)
        .without_sharded_schemas();
    let result = test.try_execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT 1").into(),
    ]);
    assert!(result.is_err());
}

/// Shard-mode tables never hash, not even into the discarded
/// first-pass route: a cold cache contributes no shard at all. A
/// value-mode table would hash the raw key into a Direct route here;
/// a shard-mode table's INSERT stays cross-shard until the lookup
/// resolves and the statement routes again.
#[test]
fn test_lookup_result_shard_never_hashes() {
    use crate::frontend::router::sharding::shard_value;
    use pgdog_config::DataType;

    let tables = shard_mode_tables();
    let mut test = QueryParserTest::new()
        .with_sharded_tables(tables)
        .without_sharded_schemas();

    let command = test.execute(vec![
        Query::new("INSERT INTO some_table (organization_id, value) VALUES ('org_child', 1)")
            .into(),
    ]);

    // The lookup is pending, and the first-pass route is NOT the
    // hash of the raw key: the raw key was never hashed at all.
    assert_eq!(command.route().pending_lookups().len(), 1);
    let hashed = shard_value("org_child", &DataType::Varchar, 2, &vec![], 0);
    assert_ne!(command.route().shard(), &hashed);
    assert_eq!(command.route().shard(), &Shard::All);
}

/// Shard-mode translations resolved for the statement route the
/// second pass without the cache.
#[test]
fn test_lookup_result_shard_routes_from_resolved() {
    use crate::frontend::router::sharding::{LookupTable, ResolvedLookups};

    let key = LookupTable {
        schema: None,
        name: None,
        column: "organization_id".into(),
    };
    let mut resolved = ResolvedLookups::default();
    resolved.insert(key, "org_child".into(), "1".into());

    // Cache is fresh and empty: only the resolved translations route.
    let mut test = QueryParserTest::new()
        .with_sharded_tables(shard_mode_tables())
        .without_sharded_schemas()
        .with_resolved_lookups(resolved);

    let command = test.execute(vec![
        Query::new("SELECT * FROM some_table WHERE organization_id = 'org_child'").into(),
    ]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(command.route().shard(), &Shard::Direct(1));

    let command = test.execute(vec![
        Query::new("/* pgdog_sharding_key: 'org_child' */ SELECT 1").into(),
    ]);
    assert!(command.route().pending_lookups().is_empty());
    assert_eq!(command.route().shard(), &Shard::Direct(1));
}
