use std::collections::HashSet;

use crate::net::FrontendPid;
use crate::net::parameter::test::new_test;

use super::*;

mod setup;
pub(crate) use setup::*;

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
