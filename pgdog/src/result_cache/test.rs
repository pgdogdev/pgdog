use crate::{
    frontend::ClientRequest,
    net::messages::{Protocol, Query as QueryMsg},
    net::ProtocolMessage,
};

use super::CacheableRequest;

#[test]
fn cacheable_request_only_simple_select_read_route() {
    let mut req = ClientRequest::new();
    req.messages
        .push(ProtocolMessage::Query(QueryMsg::new("SELECT 1")));

    // Default route is write, so not cacheable.
    assert!(CacheableRequest::from_client_request(&req).is_none());
}

