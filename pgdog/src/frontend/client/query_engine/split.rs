use crate::{
    frontend::ClientRequest,
    net::{Flush, Protocol},
};

use super::*;

impl QueryEngine {
    /// Check if the client request contains multiple
    /// extended protocol queries, and rewrite the request
    /// in a way the engine can handle them, one at a time.
    pub(super) fn split_extended_check(
        client_request: &ClientRequest,
    ) -> Result<Option<QueryEngineResult>, Error> {
        let req_count = client_request.iter().filter(|m| m.code() == 'E').count();
        if req_count <= 1 {
            return Ok(None);
        }

        let mut requests: Vec<ClientRequest> = vec![];
        let mut current_request = ClientRequest::default();

        for message in client_request.iter() {
            let code = message.code();
            match code {
                // Parse, Bind, Describe, Close typically refer
                // to the same statement.
                //
                // TODO: they don't actually have to.
                'P' | 'B' | 'D' | 'C' => {
                    current_request.push(message.clone());
                }

                // Flush typically indicates the end of the request.
                // We use it for request separation so we're only adding
                // it if we haven't already. We also don't want to send requests
                // that contain Flush only since they will get stuck.
                'H' => {
                    if let Some(last_message) = current_request.last()
                        && last_message.code() != 'H'
                    {
                        current_request.push(message.clone());
                    }
                }

                // Execute is the boundary between requests. Each request
                // can go to different shard, hence the splice.
                'E' => {
                    current_request.messages.push(message.clone());
                    current_request.messages.push(Flush.into());
                    requests.push(std::mem::take(&mut current_request));
                }

                // Sync is always in its own request. This ensures
                // we can handle ReadyForQuery separately from query results.
                'S' => {
                    // Push any accumulated messages first
                    if !current_request.is_empty() {
                        requests.push(std::mem::take(&mut current_request));
                    }
                    // Sync goes in its own request
                    current_request.messages.push(message.clone());
                    requests.push(std::mem::take(&mut current_request));
                }

                c => return Err(Error::UnexpectedMessage(c)),
            }
        }

        // Collect any remaining messages that aren't followed
        // by Flush or Sync.
        if !current_request.is_empty() {
            requests.push(current_request);
        }

        Ok(Some(QueryEngineResult::ReplaySplit {
            requests,
            extended: true,
        }))
    }

    /// Drop [`ReadyForQuery`] messages from intermediate requests.
    ///
    /// # Returns
    ///
    /// `true` if the message should be dropped, `false` if it
    /// should be forwarded to the client.
    ///
    pub(super) fn split_simple_check(
        &mut self,
        context: &QueryEngineContext<'_>,
        message: &Message,
    ) -> bool {
        if context.in_multi_query_request && context.more_requests_pending {
            match message.code() {
                'E' => {
                    self.pipeline_error = true;
                    false
                }
                'Z' => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Skip requests until there are no more pending.
    ///
    /// This allows the caller to execute the last request in the pipeline which
    /// is supposed to be a [`crate::net::Sync`].
    ///
    /// # Returns
    ///
    /// `true` if in error state, `false` if all is good.
    ///
    pub(super) fn in_pipeline_error_state(&mut self, context: &QueryEngineContext<'_>) -> bool {
        // Error in extended protocol.
        if self.backend.out_of_sync() {
            self.pipeline_error = true;
        }

        // For multi-query requests, only reset the error state
        // after the last request has been rejected.
        if context.in_multi_query_request {
            let in_error = self.pipeline_error;

            if !context.more_requests_pending {
                self.pipeline_error = false;
            }

            in_error
        } else if context.more_requests_pending {
            // Extended protocol requests will send a final `Sync`,
            // this is when we'll reset the state back.
            self.pipeline_error
        } else {
            self.pipeline_error = false;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::*;

    fn split_extended(client_request: &ClientRequest) -> Vec<ClientRequest> {
        match QueryEngine::split_extended_check(client_request)
            .expect("extended request splitting should succeed")
        {
            Some(QueryEngineResult::ReplaySplit { requests, .. }) => requests,
            Some(_) => panic!("expected an extended request split"),
            None => panic!("expected request to be split"),
        }
    }

    #[test]
    fn test_split_extended_separates_requests() {
        let messages = vec![
            ProtocolMessage::from(Parse::named("start", "BEGIN")),
            Bind::new_statement("start").into(),
            Execute::new().into(),
            Parse::named("test", "SELECT $1").into(),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Describe::new_statement("test").into(),
            Sync::new().into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = split_extended(&req);
        assert_eq!(splice.len(), 4);

        // First slice should contain: Parse("start"), Bind("start"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 4);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'B'); // Bind
        assert_eq!(first_slice[2].code(), 'E'); // Execute
        assert_eq!(first_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &first_slice[0] {
            assert_eq!(parse.name(), "start");
            assert_eq!(parse.query(), "BEGIN");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &first_slice[1] {
            assert_eq!(bind.statement(), "start");
        } else {
            panic!("Expected Bind message");
        }

        // Second slice should contain: Parse("test"), Bind("test"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 4);
        assert_eq!(second_slice[0].code(), 'P'); // Parse
        assert_eq!(second_slice[1].code(), 'B'); // Bind
        assert_eq!(second_slice[2].code(), 'E'); // Execute
        assert_eq!(second_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &second_slice[0] {
            assert_eq!(parse.name(), "test");
            assert_eq!(parse.query(), "SELECT $1");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &second_slice[1] {
            assert_eq!(bind.statement(), "test");
        } else {
            panic!("Expected Bind message");
        }

        // Third slice should contain: Describe("test")
        let third_slice = &splice[2];
        assert_eq!(third_slice.len(), 1);
        assert_eq!(third_slice[0].code(), 'D'); // Describe

        // Fourth slice should contain: Sync (always separate)
        let fourth_slice = &splice[3];
        assert_eq!(fourth_slice.len(), 1);
        assert_eq!(fourth_slice[0].code(), 'S'); // Sync
    }

    #[test]
    fn test_split_extended_ignores_single_execute() {
        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT $1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync.into(),
        ];
        let req = ClientRequest::from(messages);
        let result = QueryEngine::split_extended_check(&req)
            .expect("checking an extended request should succeed");
        assert!(result.is_none());
    }

    #[test]
    fn test_split_extended_adds_flush_after_each_execute() {
        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT 1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            ProtocolMessage::from(Parse::named("test_1", "SELECT 2")),
            Bind::new_statement("test_1").into(),
            Execute::new().into(),
            Flush.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = split_extended(&req);
        assert_eq!(splice.len(), 2);

        // First slice: Parse("test"), Bind("test"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 4);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'B'); // Bind
        assert_eq!(first_slice[2].code(), 'E'); // Execute
        assert_eq!(first_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &first_slice[0] {
            assert_eq!(parse.name(), "test");
            assert_eq!(parse.query(), "SELECT 1");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &first_slice[1] {
            assert_eq!(bind.statement(), "test");
        } else {
            panic!("Expected Bind message");
        }

        // Second slice: Parse("test_1"), Bind("test_1"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 4);
        assert_eq!(second_slice[0].code(), 'P'); // Parse
        assert_eq!(second_slice[1].code(), 'B'); // Bind
        assert_eq!(second_slice[2].code(), 'E'); // Execute
        assert_eq!(second_slice[3].code(), 'H'); // Flush
        if let ProtocolMessage::Parse(parse) = &second_slice[0] {
            assert_eq!(parse.name(), "test_1");
            assert_eq!(parse.query(), "SELECT 2");
        } else {
            panic!("Expected Parse message");
        }
        if let ProtocolMessage::Bind(bind) = &second_slice[1] {
            assert_eq!(bind.statement(), "test_1");
        } else {
            panic!("Expected Bind message");
        }

        assert_eq!(
            splice
                .first()
                .and_then(|request| request.messages.last())
                .map(Protocol::code),
            Some('H')
        );
        assert_eq!(
            splice
                .iter()
                .map(|s| s.messages.iter().filter(|p| p.code() == 'H').count())
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn test_split_extended_preserves_flush_and_separates_sync() {
        // Test Parse, Describe, Flush, Bind, Execute, Bind, Execute, Sync sequence
        let messages = vec![
            Parse::named("stmt", "SELECT $1").into(),
            Describe::new_statement("stmt").into(),
            Flush.into(),
            Bind::new_statement("stmt").into(),
            Execute::new().into(),
            Bind::new_statement("stmt").into(),
            Execute::new().into(),
            Sync::new().into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = split_extended(&req);
        assert_eq!(splice.len(), 3);

        // First slice should contain: Parse("stmt"), Describe("stmt"), Flush, Bind("stmt"), Execute, Flush
        let first_slice = &splice[0];
        assert_eq!(first_slice.len(), 6);
        assert_eq!(first_slice[0].code(), 'P'); // Parse
        assert_eq!(first_slice[1].code(), 'D'); // Describe
        assert_eq!(first_slice[2].code(), 'H'); // Flush (should be the original Flush)
        assert_eq!(first_slice[3].code(), 'B'); // Bind
        assert_eq!(first_slice[4].code(), 'E'); // Execute
        assert_eq!(first_slice[5].code(), 'H'); // Flush (added by splice logic)

        // Second slice should contain: Bind("stmt"), Execute, Flush
        let second_slice = &splice[1];
        assert_eq!(second_slice.len(), 3);
        assert_eq!(second_slice[0].code(), 'B'); // Bind
        assert_eq!(second_slice[1].code(), 'E'); // Execute
        assert_eq!(second_slice[2].code(), 'H'); // Flush

        // Third slice should contain: Sync (always separate)
        let third_slice = &splice[2];
        assert_eq!(third_slice.len(), 1);
        assert_eq!(third_slice[0].code(), 'S'); // Sync
    }
}
