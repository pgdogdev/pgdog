use pgdog_config::{QueryParserLevel, ReadWriteSplit};

use crate::{config::config, frontend::router::parser::Shard, net::Query};

use super::setup::QueryParserTest;

const QUERY: &str = "SELECT 1";

fn parser_disabled_config() -> pgdog_config::ConfigAndUsers {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::SessionControl;
    config
}

fn single_shard() -> QueryParserTest {
    QueryParserTest::new_single_shard(&parser_disabled_config())
}

fn assert_read(mut test: QueryParserTest) {
    let command = test.execute(vec![Query::new(QUERY).into()]);

    assert!(command.route().is_read());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

fn assert_write(mut test: QueryParserTest) {
    let command = test.execute(vec![Query::new(QUERY).into()]);

    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn read_only_cluster_routes_to_replica() {
    assert_read(
        QueryParserTest::new_single_replica(&parser_disabled_config())
            .with_param("pgdog.role", "primary")
            .with_rw_split(ReadWriteSplit::PreferPrimary),
    );
}

#[test]
fn write_only_cluster_routes_to_primary() {
    assert_write(
        QueryParserTest::new_single_primary(&parser_disabled_config())
            .with_param("pgdog.role", "replica")
            .with_rw_split(ReadWriteSplit::ExcludePrimary),
    );
}

#[test]
fn role_replica_routes_to_replica() {
    assert_read(single_shard().with_param("pgdog.role", "replica"));
}

#[test]
fn role_primary_routes_to_primary() {
    assert_write(single_shard().with_param("pgdog.role", "primary"));
}

#[test]
fn role_auto_routes_to_primary() {
    assert_write(single_shard().with_param("pgdog.role", "auto"));
}

#[test]
fn prefer_primary_routes_to_primary() {
    assert_write(single_shard().with_rw_split(ReadWriteSplit::PreferPrimary));
}

#[test]
fn prefer_replica_routes_to_replica() {
    assert_read(single_shard().with_rw_split(ReadWriteSplit::ExcludePrimary));
}

#[test]
fn default_routes_to_primary() {
    assert_write(single_shard());
}
