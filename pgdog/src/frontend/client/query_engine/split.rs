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

        Ok(Some(QueryEngineResult::ReplaySplitExtended(requests)))
    }

    /// Drop a [`ReadyForQuery`] message if it's part
    /// of a simple query pipeline we split and it's not
    /// the last one sent by the server.
    ///
    /// # Returns
    ///
    /// `true` if the message should be dropped, `false` if it
    /// should be forwarded to the client.
    ///
    pub(super) fn split_simple_check(
        &self,
        context: &QueryEngineContext<'_>,
        message: &Message,
    ) -> bool {
        message.code() == 'Z' && context.multi_simple_query_request && context.more_requests_pending
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::*;

    #[test]
    fn test_request_splice() {
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
        let splice = QueryEngine::split_extended_check(&req);
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

        let messages = vec![
            ProtocolMessage::from(Parse::named("test", "SELECT $1")),
            Bind::new_statement("test").into(),
            Execute::new().into(),
            Sync.into(),
        ];
        let req = ClientRequest::from(messages);
        let splice = req.spliced().unwrap();
        assert!(splice.is_empty());

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
        let splice = req.spliced().unwrap();
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

        assert_eq!(splice.first().unwrap().messages.last().unwrap().code(), 'H');
        assert_eq!(
            splice
                .iter()
                .map(|s| s.messages.iter().filter(|p| p.code() == 'H').count())
                .sum::<usize>(),
            2
        );

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
        let splice = req.spliced().unwrap();
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
