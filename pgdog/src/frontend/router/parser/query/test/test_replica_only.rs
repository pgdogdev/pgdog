use std::time::SystemTime;

use pgdog_config::{ConfigAndUsers, Database, QueryParserLevel, Role, User};
use pgdog_stats::LsnStats;

use crate::{
    backend::{
        Cluster, databases,
        pool::{Pool, RoleTarget},
        replication::publisher::Lsn,
    },
    net::{Bind, Execute, Parse, ProtocolMessage, Query, Sync},
};

use super::setup::QueryParserTest;

fn query(sql: &str, protocol: &str) -> Vec<ProtocolMessage> {
    match protocol {
        "simple" => vec![Query::new(sql).into()],
        "extended" => vec![
            Parse::new_anonymous(sql).into(),
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync.into(),
        ],
        "prepared" => vec![
            Parse::named(sql, sql).into(),
            Bind::new_statement(sql).into(),
            Execute::new().into(),
            Sync.into(),
        ],
        _ => unreachable!("unknown test protocol"),
    }
}

/// The cluster resolves the primary at checkout, so the route ignores the topology.
fn assert_transaction_route(cluster: &Cluster) {
    for protocol in ["simple", "extended", "prepared"] {
        let mut test = QueryParserTest::new().with_cluster(cluster.clone());
        test.execute(query("BEGIN", protocol));
        let mut test = test.in_transaction(true);
        let command = test.execute(query("SELECT 1", protocol));
        assert_eq!(
            command.route().role_target(),
            RoleTarget::Primary,
            "{protocol}: {command:?}"
        );
        test.execute(query("COMMIT", protocol));
    }
}

fn set_replica(pool: &Pool, replica: bool) {
    pool.set_lsn_stats(
        LsnStats {
            replica,
            lsn: Lsn::from_i64(100),
            offset_bytes: 100,
            fetched: SystemTime::now(),
            ..Default::default()
        }
        .into(),
    );
}

#[test]
fn test_replica_only_transactions_after_role_detection() {
    for parser in [
        QueryParserLevel::On,
        QueryParserLevel::Auto,
        QueryParserLevel::Off,
    ] {
        let mut config = ConfigAndUsers::default();
        config.config.general.query_parser = parser;
        config
            .users
            .users
            .push(User::new("pgdog", "pgdog", "pgdog"));
        for host in ["localhost", "127.0.0.1"] {
            config.config.databases.push(Database {
                name: "pgdog".into(),
                host: host.into(),
                role: Role::Auto,
                ..Default::default()
            });
        }
        let cluster = databases::from_config(&config)
            .cluster(("pgdog", "pgdog"))
            .expect("cluster exists");
        let shard = &cluster.shards()[0];
        let pools = shard.pools();
        assert!(cluster.has_primary());
        assert_transaction_route(&cluster);

        set_replica(&pools[0], true);
        shard.redetect_roles();
        assert!(cluster.has_primary(), "one role is still unknown");

        set_replica(&pools[1], true);
        shard.redetect_roles();
        assert!(
            !cluster.has_primary(),
            "all configured servers are replicas"
        );
        assert_transaction_route(&cluster);

        set_replica(&pools[1], false);
        shard.redetect_roles();
        assert!(cluster.has_primary(), "a replica was promoted");
        assert_transaction_route(&cluster);

        set_replica(&pools[1], true);
        shard.redetect_roles();
        assert!(!cluster.has_primary());
        assert_transaction_route(&cluster);
    }
}

#[test]
fn test_static_replica_only_transactions_with_parser_enabled() {
    let mut config = ConfigAndUsers::default();
    config.config.general.query_parser = QueryParserLevel::On;
    let test = QueryParserTest::new_single_replica(&config);
    assert_transaction_route(test.cluster());
}
