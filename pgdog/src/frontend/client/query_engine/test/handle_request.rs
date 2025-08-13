use crate::config::test::{load_test_replicas, load_test_sharded};
use crate::frontend::client::query_engine::test::Stream;
use crate::frontend::comms::comms;
use crate::frontend::router::parser::Shard;
use crate::frontend::{ClientRequest, Router};
use crate::net::{Parameter, Parameters, ProtocolMessage, Query};

use super::super::engine_impl::*;

#[tokio::test]
async fn test_basic() {
    load_test_replicas();

    let params = Parameters::from(vec![Parameter {
        name: "user".into(),
        value: "pgdog".into(),
    }]);

    let mut engine = QueryEngine::new(comms(), params, false, &None).unwrap();
    let request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new("SELECT 1"))]);
    let mut router = Router::new();
    let mut stream = Stream::default();

    for _ in 0..5 {
        engine
            .handle_request(&request, &mut router, &mut stream)
            .await
            .unwrap();
        assert!(!router.routed());
    }

    assert_eq!(stream.messages.len(), 5 * 4);
}

#[tokio::test]
async fn test_sharded() {
    load_test_sharded();
    crate::logger();

    let params = Parameters::from(vec![
        Parameter {
            name: "user".into(),
            value: "pgdog".into(),
        },
        Parameter {
            name: "database".into(),
            value: "pgdog_sharded".into(),
        },
    ]);

    let mut engine = QueryEngine::new(comms(), params, false, &None).unwrap();

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
