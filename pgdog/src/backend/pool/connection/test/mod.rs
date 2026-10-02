use std::collections::HashSet;

use crate::net::FrontendPid;
use crate::net::parameter::test::new_test;

use super::*;

mod setup;
pub(crate) use setup::*;

#[tokio::test]
async fn two_pc_subset_commit_counts() {
    use crate::{
        frontend::{
            BufferedQuery,
            client::query_engine::{TwoPcPhase, TwoPcTransaction},
        },
        net::Parameters,
    };

    let mut connection = test_connection();
    connection
        .start_transaction(false, BufferedQuery::Query(Query::new("BEGIN")))
        .unwrap();
    let pid = FrontendPid::new();
    for shard in [1, 2] {
        connection
            .connect(&Request::default(), &route(Shard::Direct(shard), false))
            .await
            .unwrap();
        connection
            .link_client(pid, &Parameters::default())
            .await
            .unwrap();
    }
    let transaction = TwoPcTransaction::new();
    connection
        .two_pc(transaction, TwoPcPhase::Phase1, false)
        .await
        .unwrap();
    connection
        .two_pc(transaction, TwoPcPhase::Phase2, false)
        .await
        .unwrap();

    let Binding::MultiShard(servers) = &connection.binding else {
        panic!("expected multi-shard binding");
    };
    for server in servers.iter() {
        assert_eq!(server.stats().total().transactions_2pc, 1);
    }
}

#[tokio::test]
async fn two_pc_missing_participants_not_counted() {
    use crate::frontend::client::query_engine::{TwoPcPhase, TwoPcTransaction};

    let mut connection = test_connection();
    connection
        .connect(&Request::default(), &route(Shard::All, false))
        .await
        .unwrap();
    connection
        .two_pc(TwoPcTransaction::new(), TwoPcPhase::Phase2, true)
        .await
        .unwrap();

    let Binding::MultiShard(servers) = &connection.binding else {
        panic!("expected multi-shard binding");
    };
    let commits: usize = servers
        .iter()
        .map(|server| server.stats().total().transactions_2pc)
        .sum();
    assert_eq!(commits, 0, "missing prepared transactions are not commits");
}

#[tokio::test]
async fn two_pc_recovery_subset() {
    use crate::frontend::{
        BufferedQuery,
        client::query_engine::{TwoPcPhase, TwoPcTransaction},
    };
    use crate::net::Parameters;
    let mut connection = test_connection();
    connection
        .start_transaction(false, BufferedQuery::Query(Query::new("BEGIN")))
        .unwrap();
    let pid = FrontendPid::new();
    for shard in [1, 2] {
        connection
            .connect(&Request::default(), &route(Shard::Direct(shard), false))
            .await
            .unwrap();
        connection
            .link_client(pid, &Parameters::default())
            .await
            .unwrap();
    }
    let transaction = TwoPcTransaction::new();
    connection
        .two_pc(transaction, TwoPcPhase::Phase1, false)
        .await
        .unwrap();
    let cluster = connection.cluster().unwrap();
    let mut recovery = Connection::new(
        &cluster.identifier().user,
        &cluster.identifier().database,
        false,
    )
    .unwrap();
    recovery
        .connect(&Request::default(), &route(Shard::All, false))
        .await
        .unwrap();
    let recovered = recovery
        .two_pc(transaction, TwoPcPhase::Rollback, true)
        .await;
    // Clean up using the original participant order even when recovery failed.
    connection
        .two_pc(transaction, TwoPcPhase::Rollback, true)
        .await
        .unwrap();
    assert!(recovered.is_ok(), "recovery failed: {recovered:?}");
}

#[tokio::test]
async fn test_connection_upgrade() {
    async fn assert_param(server: &mut LinkedServer) -> bool {
        let param = server
            .fetch_all::<String>("SHOW application_name")
            .await
            .unwrap()
            .pop()
            .unwrap();
        param == "test_connection_connect_upgrade"
    }

    let mut connection = test_connection();
    let pid = FrontendPid::new();
    let params = new_test("test_connection_connect_upgrade");

    connection
        .connect(&Request::default(), &route(Shard::Direct(0), true))
        .await
        .unwrap();

    assert_eq!(1, connection.connected_servers());
    assert!(!connection.in_buffered_transaction());

    connection.link_client(pid, &params).await.unwrap();

    if let Binding::Direct(ref mut direct) = connection.binding {
        assert!(
            assert_param(&mut direct.server).await,
            "direct-to-shard link_client should sync params"
        );
    } else {
        panic!("direct-to-shard should use direct binding");
    }

    connection
        .connect(&Request::default(), &route(Shard::Direct(1), true))
        .await
        .unwrap();
    assert_eq!(2, connection.connected_servers());
    connection.link_client(pid, &params).await.unwrap();

    if let Binding::MultiShard(ref mut multi) = connection.binding {
        for server in multi.iter_mut() {
            assert!(
                assert_param(server).await,
                "cross-shard upgrade should sync params"
            );
        }
    } else {
        panic!("cross-shard should use multi binding");
    }

    connection
        .connect(&Request::default(), &route(Shard::All, false))
        .await
        .unwrap();
    assert_eq!(3, connection.connected_servers());

    if let Binding::MultiShard(ref mut multi) = connection.binding {
        assert_eq!(
            3,
            multi
                .iter_mut()
                .map(|server| server.shard)
                .collect::<HashSet<_>>()
                .len(),
            "shard numbers should be unique"
        );
        assert!(
            multi.iter().all(|server| {
                server
                    .params()
                    .contains_key("default_transaction_read_only")
            }),
            "read change mid connection preserves read preference"
        );
    } else {
        panic!("cross-shard should use multi binding");
    }
}
