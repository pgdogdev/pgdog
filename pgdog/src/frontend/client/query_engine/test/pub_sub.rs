use std::io::ErrorKind;

use crate::{
    expect_message,
    net::{BindComplete, CommandComplete, Parameters, ParseComplete},
};

use super::prelude::*;

#[tokio::test]
async fn extended_unlisten_flush_does_not_emit_ready_for_query() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client.client.client_request = ClientRequest::from(vec![
        Parse::new_anonymous("UNLISTEN pgdog_pipeline_listen").into(),
        Bind::new_statement("").into(),
        Execute::new().into(),
        Flush.into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);
    client
        .engine
        .unlisten(&mut context, "pgdog_pipeline_listen")
        .await
        .unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "UNLISTEN"
    );

    let mut unexpected = [0];
    let error = client
        .conn
        .try_read(&mut unexpected)
        .expect_err("Flush-only extended response must not include ReadyForQuery");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
}
