use crate::config::test::{load_test, load_test_replicas, load_test_sharded};
use crate::frontend::client::query_engine::test::Stream;
use crate::frontend::comms::comms;
use crate::frontend::router::parser::Shard;
use crate::frontend::{ClientRequest, PreparedStatements, Router};
use crate::net::{Parameter, Parameters, ProtocolMessage, Query};

use super::super::engine_impl::*;

#[tokio::test]
async fn test_basic() {
    load_test();

    let mut params = Parameters::from(vec![Parameter {
        name: "user".into(),
        value: "pgdog".into(),
    }]);
    let mut prepard = PreparedStatements::new();
    let mut router = Router::new();
    let mut stream = Stream::default();
    let mut engine = QueryEngine::new(comms(), &mut params, &mut prepard, false, &None).unwrap();

    for _ in 0.. 5 {
        for (query, in_transaction) in [("BEGIN", true), ("SELECT 1", true), ("COMMIT", false)] {
            let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(query))]);
            engine.handle_request(&request, &mut router, &mut stream).await.unwrap();
            assert_eq!(engine.transaction.started(), in_transaction);
        }
    }
}

#[tokio::test]
async fn test_basic_with_replicas() {
    load_test_replicas();

    let mut params = Parameters::from(vec![Parameter {
        name: "user".into(),
        value: "pgdog".into(),
    }]);
    let mut prepard = PreparedStatements::new();

    let mut engine = QueryEngine::new(comms(), &mut params, &mut prepard, false, &None).unwrap();
    let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new("SELECT 1"))]);
    let mut router = Router::new();
    let mut stream = Stream::default();

    for _ in 0..5 {
        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();
        assert!(!router.routed());
        assert!(!engine.backend.connected());
        assert!(!engine.transaction.started());
    }

    assert_eq!(stream.messages.len(), 5 * 4);

    for _ in 0.. 5 {
        for (query, in_transaction) in [("BEGIN", true), ("SELECT 1", true), ("COMMIT", false)] {
            let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(query))]);
            engine.handle_request(&request, &mut router, &mut stream).await.unwrap();
            assert_eq!(engine.transaction.started(), in_transaction);
        }
    }
}

#[tokio::test]
async fn test_sharded() {
    load_test_sharded();
    crate::logger();

    let mut params = Parameters::from(vec![
        Parameter {
            name: "user".into(),
            value: "pgdog".into(),
        },
        Parameter {
            name: "database".into(),
            value: "pgdog_sharded".into(),
        },
    ]);
    let mut prepard = PreparedStatements::new();

    let mut engine = QueryEngine::new(comms(), &mut params, &mut prepard, false, &None).unwrap();

    let mut router = Router::new();
    let mut stream = Stream::default();

    for _ in 0..5 {
        let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new("BEGIN"))]);
        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();
        assert!(engine.transaction.started());
        assert!(engine.transaction.buffered());
        assert!(!engine.backend.connected());

        let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM sharded WHERE id = 1",
        ))]);

        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();

        assert!(engine.transaction.started());
        assert!(!engine.transaction.buffered());
        assert!(router.routed());
        assert_eq!(router.route().shard(), &Shard::Direct(0));
        assert!(engine.backend.connected());

        let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(
            "SELECT * FROM sharded",
        ))]);

        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();

        assert!(router.routed());
        assert_eq!(router.route().shard(), &Shard::All);

        let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new("COMMIT"))]);

        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();
        assert!(!engine.backend.connected());

        assert!(!engine.transaction.started());
    }
}
