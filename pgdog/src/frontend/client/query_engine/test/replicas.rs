use crate::{
    backend::databases::databases,
    config::Role,
    net::{Parameters, ToBytes},
};

use super::prelude::*;

/// Test that queries are distributed across primary and replica via round-robin,
/// and that pool stats are tracked correctly.
#[tokio::test]
async fn test_round_robin_with_replicas() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    let mut len_sent = 0;
    let mut len_recv = 0;

    // Write goes to primary.
    let query = Query::new("CREATE TABLE IF NOT EXISTS test_round_robin_replicas (id BIGINT)");
    len_sent += query.to_bytes().unwrap().len();
    client.send_simple(query).await;
    for msg in client.read_until('Z').await.unwrap() {
        len_recv += msg.len();
    }

    // 26 read queries via extended protocol, round-robined across primary + replica.
    for _ in 0..26 {
        let parse = Parse::named("test", "SELECT * FROM test_round_robin_replicas");
        let bind = Bind::new_statement("test");
        let execute = Execute::new();
        let sync = Sync;
        len_sent += parse.to_bytes().unwrap().len();
        len_sent += bind.to_bytes().unwrap().len();
        len_sent += execute.to_bytes().unwrap().len();
        len_sent += sync.to_bytes().unwrap().len();

        client.send(parse).await;
        client.send(bind).await;
        client.send(execute).await;
        client.send(sync).await;
        client.try_process().await.unwrap();

        for msg in client.read_until('Z').await.unwrap() {
            len_recv += msg.len();
        }
    }

    let healthcheck_len_recv = 5 + 6; // Empty query response + ready for query
    let healthcheck_len_sent = Query::new(";").len(); // Health check query

    let pools = databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools_with_roles();

    let mut pool_recv = 0isize;
    let mut pool_sent = 0isize;

    for (role, pool) in pools {
        let state = pool.state();
        let idle = state.idle;

        pool_recv += state.stats.counts.received as isize;
        pool_sent += state.stats.counts.sent as isize;

        match role {
            Role::Primary => {
                // 1 write (CREATE TABLE) + 13 reads = 14 assignments.
                assert_eq!(state.stats.counts.server_assignment_count, 14);
                assert_eq!(state.stats.counts.bind_count, 13);
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert_eq!(state.stats.counts.healthchecks, idle);
                // Parse count depends on number of idle connections (prepared statement sync).
                assert!(state.stats.counts.parse_count >= idle);
                assert!(state.stats.counts.parse_count <= idle + 1);
                pool_recv -= (healthcheck_len_recv * state.stats.counts.healthchecks) as isize;
            }
            Role::Replica => {
                // 13 reads.
                assert_eq!(state.stats.counts.server_assignment_count, 13);
                assert_eq!(state.stats.counts.bind_count, 13);
                assert_eq!(state.stats.counts.rollbacks, 0);
                assert!(state.stats.counts.healthchecks <= idle + 1);
                assert!(state.stats.counts.parse_count <= idle + 1);
                pool_sent -= (healthcheck_len_sent * state.stats.counts.healthchecks) as isize;
            }
            Role::Auto => unreachable!("role auto"),
        }

        assert_eq!(
            state.stats.counts.query_count,
            state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
        );
        assert_eq!(
            state.stats.counts.xact_count,
            state.stats.counts.server_assignment_count + state.stats.counts.healthchecks
        );
    }

    assert!(pool_sent <= len_sent as isize);
    assert!(pool_recv <= len_recv as isize);
}
