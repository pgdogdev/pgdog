use tracing::error;

use crate::{
    net::{Message, Protocol},
    stats::memory::MemoryUsage,
};

use super::super::Error;
use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Forward,
    Ignore,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ExecutionCode {
    ReadyForQuery,
    ExecutionCompleted,
    ParseComplete,
    BindComplete,
    CloseComplete,
    DescriptionOrNothing,
    Copy,
    Error,
    Untracked,
}

impl MemoryUsage for ExecutionCode {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<ExecutionCode>()
    }
}

impl ExecutionCode {
    fn extended(&self) -> bool {
        matches!(self, Self::ParseComplete | Self::BindComplete)
    }
}

impl From<char> for ExecutionCode {
    fn from(value: char) -> Self {
        match value {
            'Z' => Self::ReadyForQuery,
            'C' | 's' | 'I' => Self::ExecutionCompleted, // CommandComplete or PortalSuspended
            '1' => Self::ParseComplete,
            '2' => Self::BindComplete,
            '3' => Self::CloseComplete,
            'T' | 'n' | 't' => Self::DescriptionOrNothing,
            'G' | 'c' | 'f' => Self::Copy,
            'E' => Self::Error,
            _ => Self::Untracked,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionItem {
    Code(ExecutionCode),
    Ignore(ExecutionCode),
}

impl MemoryUsage for ExecutionItem {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl ExecutionItem {
    fn extended(&self) -> bool {
        match self {
            Self::Code(code) | Self::Ignore(code) => code.extended(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProtocolState {
    queue: VecDeque<ExecutionItem>,
    simulated: VecDeque<Message>,
    extended: bool,
    out_of_sync: bool,
}

impl MemoryUsage for ProtocolState {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.queue.memory_usage()
            + self.simulated.memory_usage()
            + self.extended.memory_usage()
            + self.out_of_sync.memory_usage()
    }
}

impl ProtocolState {
    /// Add a message to the ignore list.
    ///
    /// The server will return this message, but we won't send it to the client.
    /// This is used for preparing statements that the client expects to be there
    /// but the server connection doesn't have yet.
    ///
    pub(crate) fn add_ignore(&mut self, code: impl Into<ExecutionCode>) {
        let code = code.into();
        self.extended = self.extended || code.extended();
        self.queue.push_back(ExecutionItem::Ignore(code));
    }

    /// Add a message to the execution queue. We expect this message
    /// to be returned by the server.
    pub(crate) fn add(&mut self, code: impl Into<ExecutionCode>) {
        let code = code.into();
        self.extended = self.extended || code.extended();
        self.queue.push_back(ExecutionItem::Code(code))
    }

    /// New code we expect now to arrive first.
    pub(crate) fn prepend(&mut self, code: impl Into<ExecutionCode>) {
        let code = code.into();
        self.queue.push_front(ExecutionItem::Code(code));
    }

    /// Add a message we will return to the client but the server
    /// won't send. This is used for telling the client we did something,
    /// e.g. closed a prepared statement, when we actually did not.
    pub(crate) fn add_simulated(&mut self, message: Message) {
        self.queue
            .push_back(ExecutionItem::Code(message.code().into()));
        self.simulated.push_back(message);
    }

    /// Get a simulated message from the execution queue.
    ///
    /// Returns a message only if it should be returned at the current state
    /// of the extended pipeline.
    pub fn get_simulated(&mut self) -> Option<Message> {
        let code = self.queue.front();
        let message = self.simulated.front();
        if let Some(ExecutionItem::Code(code)) = code {
            if let Some(message) = message {
                if code == &ExecutionCode::from(message.code()) {
                    let _ = self.queue.pop_front();
                    return self.simulated.pop_front();
                }
            }
        }
        None
    }

    /// Should we ignore the message we just received
    /// and not forward it to the client.
    pub fn action(&mut self, code: impl Into<ExecutionCode> + Debug) -> Result<Action, Error> {
        let code = code.into();
        match code {
            ExecutionCode::Untracked => return Ok(Action::Forward),
            ExecutionCode::Error => {
                if matches!(
                    self.queue.front(),
                    Some(ExecutionItem::Ignore(ExecutionCode::Error))
                ) {
                    // We ignore errors only for the pgdog-injected sub-request.
                    // In that case the first error is already processed and
                    // sent to the client, for the remaining expected errors
                    // we've added ignores for errors and RFQ.
                    // The error is ignored but still be logged by [backend::server] module
                    self.queue.pop_front();
                    return Ok(Action::Ignore);
                }

                // This is the first (and client-visible) error in the chain. It is forwarded
                // so the client receives exactly one Error+RFQ for their request.

                // For extended-protocol pipelines also mark out-of-sync so the connection
                // is not reused until the client re-syncs.
                if self.extended {
                    self.out_of_sync = true;
                }

                // find the first position for RFQ code to effectively
                // separate the pgdog-injected sub-request from the remaining queries
                let Some(rfq_pos) = self
                    .queue
                    .iter()
                    .position(|i| matches!(i, ExecutionItem::Code(ExecutionCode::ReadyForQuery)))
                else {
                    self.queue.clear();
                    return Ok(Action::Forward);
                };

                // broken_queue - pgdog-injected sub-request part that contains multiple requests
                // that are not be executed properly anyway, since we've got an error previously
                let broken_queue = self.queue.drain(..rfq_pos);

                // Count how many queries are expected to finish in the pgdog-injected sub-request
                // The current use case is only the Prepare + Execute messages from the [backend::server]
                // And in case the prepare fails the execute will fail as well.
                // WARN: That is not most reliable solution in case the injected set of queries
                // will extend, but it should work for now.
                let count_ignores = broken_queue
                    .filter(|i| matches!(i, ExecutionItem::Ignore(ExecutionCode::ReadyForQuery)))
                    .count();

                // For every message that we expect to run add ignore for one error and one RFQ
                // For prepare it'll be a one iteration that will create the query
                // [Ignore(RFQ), Ignore(Error), Code(RFQ)]
                for _ in 0..count_ignores {
                    self.queue
                        .push_front(ExecutionItem::Ignore(ExecutionCode::Error));
                    self.queue
                        .push_front(ExecutionItem::Ignore(ExecutionCode::ReadyForQuery));
                }

                return Ok(Action::Forward);
            }

            ExecutionCode::ReadyForQuery => {
                self.out_of_sync = false;
            }
            _ => (),
        };
        let in_queue = self.queue.pop_front().ok_or_else(|| {
            error!("Unexpected action {code:?}: queue is empty");
            Error::ProtocolOutOfSync
        })?;
        let action = match in_queue {
            // The queue is waiting for the server to send ReadyForQuery,
            // but it sent something else. That means the execution pipeline
            // isn't done. We are not tracking every single message, so this is expected.
            ExecutionItem::Code(in_queue_code) => {
                if code != ExecutionCode::ReadyForQuery
                    && in_queue_code == ExecutionCode::ReadyForQuery
                {
                    self.queue.push_front(in_queue);
                }

                Ok(Action::Forward)
            }

            // Used for preparing statements that the client expects to be there.
            ExecutionItem::Ignore(in_queue) => {
                if code == in_queue {
                    Ok(Action::Ignore)
                } else {
                    error!(?self, "Unexpected action {code:?}: expected: {in_queue:?}");

                    Err(Error::ProtocolOutOfSync)
                }
            }
        }?;

        if code == ExecutionCode::ReadyForQuery {
            self.extended = self.queue.iter().any(ExecutionItem::extended);
        }

        Ok(action)
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        self.queue.front() == Some(&ExecutionItem::Code(ExecutionCode::Copy))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub(crate) fn queue(&self) -> &VecDeque<ExecutionItem> {
        &self.queue
    }

    pub(crate) fn done(&self) -> bool {
        self.is_empty() && !self.out_of_sync
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        !self.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn in_sync(&self) -> bool {
        !self.out_of_sync
    }

    /// Check if the protocol is out of sync due to an error in extended protocol.
    pub(crate) fn out_of_sync(&self) -> bool {
        self.out_of_sync
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // ========================================
    // Simple Query Protocol Tests
    // ========================================

    #[test]
    fn test_simple_query_with_results() {
        let mut state = ProtocolState::default();
        // Simple query: SELECT * FROM users
        // Expected: RowDescription -> DataRow(s) -> CommandComplete -> ReadyForQuery
        state.add('T'); // RowDescription
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('T').unwrap(), Action::Forward);
        // DataRows are not tracked, they come between T and C
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_simple_query_no_results() {
        let mut state = ProtocolState::default();
        // Simple query: INSERT/UPDATE/DELETE
        // Expected: CommandComplete -> ReadyForQuery
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_simple_query_empty() {
        let mut state = ProtocolState::default();
        // Empty query
        // Expected: EmptyQueryResponse -> ReadyForQuery
        state.add('I'); // EmptyQueryResponse
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('I').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_simple_query_error() {
        let mut state = ProtocolState::default();
        // Query with syntax error
        // Expected: ErrorResponse -> ReadyForQuery
        state.add('C'); // CommandComplete (expected but won't arrive)
        state.add('Z'); // ReadyForQuery

        // Error clears the queue except ReadyForQuery
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert_eq!(state.len(), 1); // Only ReadyForQuery remains
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_simple_query_multiple_results() {
        let mut state = ProtocolState::default();
        // Multiple SELECT statements in one query
        // Expected: T->C->T->C->Z
        state.add('T'); // RowDescription for first query
        state.add('C'); // CommandComplete for first query
        state.add('T'); // RowDescription for second query
        state.add('C'); // CommandComplete for second query
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('T').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('T').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    // ========================================
    // Extended Query Protocol Tests
    // ========================================

    #[test]
    fn test_extended_parse_bind_execute_sync() {
        let mut state = ProtocolState::default();
        // Basic extended query: Parse -> Bind -> Execute -> Sync
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
        assert!(!state.extended);
    }

    #[test]
    fn test_extended_parse_bind_describe_execute_sync() {
        let mut state = ProtocolState::default();
        // Extended query with Describe: Parse -> Bind -> Describe -> Execute -> Sync
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete
        state.add('T'); // RowDescription from Describe
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('T').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_describe_statement_returns_nodata() {
        let mut state = ProtocolState::default();
        // Describe a statement that doesn't return data (e.g., INSERT)
        state.add('1'); // ParseComplete
        state.add('n'); // NoData from Describe
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('n').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_portal_suspended() {
        let mut state = ProtocolState::default();
        // Execute with row limit, returns PortalSuspended
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete
        state.add('s'); // PortalSuspended (partial results)
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('s').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_close_statement() {
        let mut state = ProtocolState::default();
        // Close a prepared statement
        state.add('3'); // CloseComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('3').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_pipelined_queries() {
        let mut state = ProtocolState::default();
        // Multiple queries in one pipeline: Parse->Bind->Execute->Parse->Bind->Execute->Sync
        state.add('1'); // ParseComplete #1
        state.add('2'); // BindComplete #1
        state.add('C'); // CommandComplete #1
        state.add('1'); // ParseComplete #2
        state.add('2'); // BindComplete #2
        state.add('C'); // CommandComplete #2
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    // ========================================
    // Error Handling Tests
    // ========================================

    #[test]
    fn test_extended_parse_error() {
        let mut state = ProtocolState::default();
        // Parse fails (syntax error)
        state.add('1'); // ParseComplete (expected but won't arrive)
        state.add('2'); // BindComplete (won't be reached)
        state.add('Z'); // ReadyForQuery

        // Error clears queue except ReadyForQuery
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(state.out_of_sync);
        assert_eq!(state.len(), 1); // Only ReadyForQuery remains
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(!state.out_of_sync);
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_bind_error() {
        let mut state = ProtocolState::default();
        // Parse succeeds, Bind fails
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete (expected but won't arrive)
        state.add('C'); // CommandComplete (won't be reached)
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(state.out_of_sync);
        assert_eq!(state.len(), 1);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(!state.out_of_sync);
    }

    #[test]
    fn test_extended_execute_error() {
        let mut state = ProtocolState::default();
        // Parse and Bind succeed, Execute fails
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete (expected but won't arrive)
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(state.out_of_sync);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(!state.out_of_sync);
    }

    #[test]
    fn test_simple_query_error_no_out_of_sync() {
        let mut state = ProtocolState::default();
        // Simple query error should NOT set out_of_sync
        state.add('C'); // CommandComplete (expected but won't arrive)
        state.add('Z'); // ReadyForQuery

        assert!(!state.extended);
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(!state.out_of_sync); // Simple query doesn't set out_of_sync
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
    }

    // A simple query that errors must not set out_of_sync, even when extended-
    // protocol requests are queued after it. add() currently sets extended=true
    // as soon as any ParseComplete is enqueued, causing the error arm to take
    // the extended path for the preceding simple query.
    //
    // Currently FAILS: extended=true from queuing the future extended request
    // causes out_of_sync to be set on the simple-query error.
    #[test]
    fn test_simple_query_error_before_queued_extended_request_does_not_set_out_of_sync() {
        // Setup: simple query followed by an extended query in the same queue.
        let mut state = ProtocolState::default();
        state.add('C'); // CommandComplete (simple query, won't arrive)
        state.add('Z'); // ReadyForQuery (simple query)
        state.add('1'); // ParseComplete (extended query)
        state.add('2'); // BindComplete (extended query)
        state.add('C'); // CommandComplete (extended query)
        state.add('Z'); // ReadyForQuery (extended query)

        // Simple query errors — must take the simple-query path.
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        // out_of_sync must not be set: this was a simple-query error.
        assert!(!state.out_of_sync);
        // Simple query's ReadyForQuery remains; extended entries intact.
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // simple query RFQ
                                                                 // Extended query is still processable.
        assert_eq!(state.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(state.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(state.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(state.is_empty());
    }

    // The extended flag recalculation at ReadyForQuery scans the entire remaining
    // queue, so a simple query that sits before an extended query inherits
    // extended=true after the preceding ReadyForQuery is consumed.
    //
    // Currently FAILS: after the first simple query's RFQ, extended is set to
    // true because the scan finds ParseComplete/BindComplete further in the queue.
    // The second simple query's error then incorrectly takes the extended path.
    #[test]
    fn test_simple_query_error_after_rfq_before_extended_does_not_set_out_of_sync() {
        // Setup: two simple queries, then an extended query.
        let mut state = ProtocolState::default();
        state.add('C'); // CommandComplete (simple query 1)
        state.add('Z'); // ReadyForQuery (simple query 1)
        state.add('C'); // CommandComplete (simple query 2, won't arrive)
        state.add('Z'); // ReadyForQuery (simple query 2)
        state.add('1'); // ParseComplete (extended query)
        state.add('2'); // BindComplete (extended query)
        state.add('C'); // CommandComplete (extended query)
        state.add('Z'); // ReadyForQuery (extended query)

        // Simple query 1 runs normally.
        assert_eq!(state.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery

        // Simple query 2 errors — must still take the simple-query path.
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        // out_of_sync must not be set: this was a simple-query error.
        assert!(!state.out_of_sync);
        // Simple query 2's ReadyForQuery remains; extended entries intact.
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // simple query 2 RFQ
                                                                 // Extended query is still processable.
        assert_eq!(state.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(state.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(state.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(state.is_empty());
    }

    #[test]
    fn test_extended_error_in_pipeline() {
        let mut state = ProtocolState::default();
        // Pipeline with error in middle: P->B->E->P->B->E->Sync
        // If first Execute fails, rest of pipeline still processes
        state.add('1'); // ParseComplete #1
        state.add('2'); // BindComplete #1
        state.add('C'); // CommandComplete #1 (won't arrive)
        state.add('1'); // ParseComplete #2
        state.add('2'); // BindComplete #2
        state.add('C'); // CommandComplete #2
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(state.out_of_sync);
        // After error in extended protocol, we're out of sync
        // Server still sends remaining responses but we're waiting for ReadyForQuery
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(!state.out_of_sync);
    }

    // ========================================
    // COPY Protocol Tests
    // ========================================

    #[test]
    fn test_copy_in_success() {
        let mut state = ProtocolState::default();
        // COPY FROM STDIN
        state.add('G'); // CopyInResponse
        state.add('C'); // CommandComplete (after CopyDone or CopyFail)
        state.add('Z'); // ReadyForQuery

        // Check copy_mode before consuming the message
        assert!(state.in_copy_mode());
        assert_eq!(state.action('G').unwrap(), Action::Forward);
        // After consuming 'G', we're no longer in copy mode (it's popped from queue)
        assert!(!state.in_copy_mode());
        // CopyData messages ('d') would be sent here but aren't tracked
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_copy_fail() {
        let mut state = ProtocolState::default();
        // COPY that fails
        state.add('G'); // CopyInResponse
        state.add('C'); // CommandComplete (won't arrive due to error)
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('G').unwrap(), Action::Forward);
        // Client sends CopyFail ('f')
        // Server responds with ErrorResponse
        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
    }

    // ========================================
    // Ignore Tests (for statement preparation)
    // ========================================

    #[test]
    fn test_ignore_parse_complete() {
        let mut state = ProtocolState::default();
        state.add_ignore('1');
        assert_eq!(state.action('1').unwrap(), Action::Ignore);
        assert!(state.is_empty());
    }

    #[test]
    fn test_ignore_bind_complete() {
        let mut state = ProtocolState::default();
        state.add_ignore('2');
        assert_eq!(state.action('2').unwrap(), Action::Ignore);
        assert!(state.is_empty());
    }

    #[test]
    fn test_ignore_error_behavior() {
        let mut state = ProtocolState::default();
        state.add_ignore('1');
        state.add_ignore('2');

        // When we get an error with Ignore items in queue,
        // the Error arm is triggered first (before checking queue items)
        // so it clears the queue and returns Forward, not ForwardAndRemove
        let result = state.action('E').unwrap();
        assert_eq!(result, Action::Forward);
        // Queue should be empty after error
        assert!(state.is_empty());
        // Note: The ForwardAndRemove logic in the Ignore arm (line 192-193)
        // is unreachable because Error is handled at the top of action()
        // This may be dead code or a bug in the implementation.
    }

    #[test]
    fn test_pipelined_simple_query_error_keeps_next_query_response() {
        let mut state = ProtocolState::default();
        state.add('Z'); // First simple query.
        state.add('Z'); // Second simple query.

        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert_eq!(state.len(), 2);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert_eq!(state.len(), 1);

        // The next response belongs to the second simple query.
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_ignore_wrong_code_is_out_of_sync() {
        let mut state = ProtocolState::default();
        state.add_ignore('1');
        // We expect ParseComplete but get BindComplete
        assert!(state.action('2').is_err());
    }

    // ========================================
    // Simulated Messages Tests
    // ========================================

    #[test]
    fn test_simulated_message() {
        let mut state = ProtocolState::default();
        // Create a simulated CloseComplete message
        let message = Message::new(bytes::Bytes::from(vec![b'3', 0, 0, 0, 4]));
        state.add_simulated(message.clone());

        assert_eq!(state.len(), 1);
        let retrieved = state.get_simulated();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().code(), '3');
        assert!(state.is_empty());
    }

    #[test]
    fn test_simulated_message_wrong_position() {
        let mut state = ProtocolState::default();
        let message = Message::new(bytes::Bytes::from(vec![b'3', 0, 0, 0, 4]));
        state.add('1'); // ParseComplete expected first
        state.add_simulated(message);

        // get_simulated should return None because CloseComplete is not at front
        let retrieved = state.get_simulated();
        assert!(retrieved.is_none());
        assert_eq!(state.len(), 2);
    }

    // ========================================
    // State Management Tests
    // ========================================

    #[test]
    fn test_prepend() {
        let mut state = ProtocolState::default();
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery
        state.prepend('T'); // RowDescription should come first

        assert_eq!(state.action('T').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_untracked_messages_always_forward() {
        let mut state = ProtocolState::default();
        state.add('C'); // CommandComplete

        // Untracked messages (like DataRow 'D', NoticeResponse 'N', etc.) should always forward
        // even if they're not in the queue
        assert_eq!(state.action('D').unwrap(), Action::Forward);
        assert_eq!(state.action('N').unwrap(), Action::Forward);
        assert_eq!(state.action('S').unwrap(), Action::Forward); // ParameterStatus

        // Original queue should be unchanged
        assert_eq!(state.len(), 1);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
    }

    #[test]
    fn test_ready_for_query_when_expecting_other() {
        let mut state = ProtocolState::default();
        state.add('T'); // RowDescription
        state.add('Z'); // ReadyForQuery

        // If we receive ReadyForQuery but we're expecting RowDescription first:
        // - The code sets out_of_sync = false
        // - Pops 'T' from queue
        // - Checks: is received code NOT RFQ AND expected code IS RFQ?
        // - No, received IS RFQ, so we don't push back
        // - We consume 'T' and move on, 'Z' remains in queue
        let result = state.action('Z');
        assert!(result.is_ok());
        assert_eq!(state.len(), 1); // Only 'Z' remains
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_done_when_empty_and_in_sync() {
        let mut state = ProtocolState::default();
        assert!(state.done());

        state.add('Z');
        assert!(!state.done()); // Has messages

        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.done()); // Empty and in sync
    }

    #[test]
    fn test_not_done_when_out_of_sync() {
        let mut state = ProtocolState::default();
        state.add('1'); // ParseComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('E').unwrap(), Action::Forward);
        assert!(state.out_of_sync);
        assert!(!state.done()); // Out of sync, not done even though has messages

        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.done()); // Back in sync and empty
    }

    #[test]
    fn test_out_of_sync_empty_queue() {
        let mut state = ProtocolState::default();
        // Error with no pending ReadyForQuery
        let result = state.action('E');
        assert!(result.is_ok());
        assert!(state.is_empty()); // Queue is empty
    }

    // ========================================
    // Edge Cases and Complex Scenarios
    // ========================================

    #[test]
    fn test_multiple_untracked_between_tracked() {
        let mut state = ProtocolState::default();
        state.add('T'); // RowDescription
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('T').unwrap(), Action::Forward);
        // Multiple DataRows (untracked)
        assert_eq!(state.action('D').unwrap(), Action::Forward);
        assert_eq!(state.action('D').unwrap(), Action::Forward);
        assert_eq!(state.action('D').unwrap(), Action::Forward);
        // NoticeResponse (untracked)
        assert_eq!(state.action('N').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_parameter_description() {
        let mut state = ProtocolState::default();
        // Describe a prepared statement's parameters
        state.add('1'); // ParseComplete
        state.add('t'); // ParameterDescription
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert_eq!(state.action('t').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    #[test]
    fn test_empty_queue_non_error_message() {
        let mut state = ProtocolState::default();
        // Receiving a tracked message when queue is empty should be OutOfSync
        let result = state.action('C');
        assert!(result.is_err());
    }

    #[test]
    fn test_mixed_simple_and_extended() {
        let mut state = ProtocolState::default();
        // This shouldn't happen in practice, but test state tracking
        // Simple query
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);

        // Now extended query
        state.add('1'); // ParseComplete
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Forward);
        assert!(state.extended); // Now marked as extended
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
    }

    #[test]
    fn test_copy_mode_detection() {
        let mut state = ProtocolState::default();
        assert!(!state.in_copy_mode());

        state.add('G'); // CopyInResponse
        state.add('C'); // CommandComplete
        assert!(state.in_copy_mode());

        assert_eq!(state.action('G').unwrap(), Action::Forward);
        assert!(!state.in_copy_mode()); // No longer at front

        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert!(!state.in_copy_mode());
    }

    #[test]
    fn test_has_more_messages() {
        let mut state = ProtocolState::default();
        assert!(!state.has_more_messages());

        state.add('C');
        assert!(state.has_more_messages());

        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert!(!state.has_more_messages());
    }

    #[test]
    fn test_names_cleared_on_error() {
        // This test verifies that when an error occurs, both queue AND names
        // are cleared to maintain the invariant that they stay synchronized.

        let mut state = ProtocolState::default();
        state.add_ignore('1');
        state.add_ignore('2');
        state.add_ignore('3');
        state.add('Z'); // ReadyForQuery

        // Error should clear both queue (except RFQ) and names
        assert_eq!(state.action('E').unwrap(), Action::Forward);

        // Consume the ReadyForQuery
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty()); // Queue is empty

        // Now if we add a new ignore item, it should work correctly
        // because names was also cleared
        state.add_ignore('1');
        assert_eq!(state.action('1').unwrap(), Action::Ignore);
        assert!(state.is_empty()); // Both queue and names should be empty

        // Verify we can continue using the state normally
        state.add_ignore('2');
        state.add('C');
        state.add('Z');

        // Process normally
        assert_eq!(state.action('2').unwrap(), Action::Ignore);
        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    // Double action('c') for server CopyDone

    // Safe path: Code(ReadyForQuery) backstop makes the double action('c') call idempotent.
    #[test]
    fn test_copydone_double_action_safe_with_rfq_backstop() {
        let mut state = ProtocolState::default();
        // 1. Queue: CopyOut slot + RFQ backstop (from Sync).
        state.add('G'); // CopyOut
        state.add('Z'); // ReadyForQuery backstop

        // 2. First action('c'): pops CopyOut; RFQ backstop untouched.
        assert_eq!(state.action('c').unwrap(), Action::Forward);
        assert_eq!(state.len(), 1);

        // 3. Second action('c'): sees RFQ at front; pushes it back (idempotent).
        assert_eq!(state.action('c').unwrap(), Action::Forward);
        assert_eq!(state.len(), 1); // RFQ still present for the server's ReadyForQuery
    }

    // Failure path: no Code(ReadyForQuery) backstop — second action('c') hits empty queue.
    #[test]
    fn test_copydone_double_action_oos_without_rfq_backstop() {
        let mut state = ProtocolState::default();
        // 1. Queue: Execute + Flush (no Sync) — no RFQ backstop.
        state.add('C'); // ExecutionCompleted

        // 2. First action('c'): pops ExecutionCompleted; queue empty.
        assert_eq!(state.action('c').unwrap(), Action::Forward);
        assert!(state.is_empty());

        // 3. Second action('c'): empty queue → ProtocolOutOfSync.
        assert!(state.action('c').is_err());
    }

    // Stale RFQ arrives before injected ParseComplete — Ignore arm rejects the mismatch.
    #[test]
    fn test_stale_rfq_hits_ignore_parsecomplete() {
        let mut state = ProtocolState::default();
        // 1. pgdog injects Parse; queue: [Ignore(ParseComplete), BindComplete, CommandComplete, RFQ].
        state.add_ignore('1'); // ParseComplete — injected
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        // Stale RFQ from prior cycle arrives before ParseComplete.
        // ReadyForQuery != ParseComplete → ProtocolOutOfSync.
        assert!(
            state.action('Z').is_err(),
            "stale RFQ against Ignore(ParseComplete) must produce ProtocolOutOfSync"
        );
    }

    // Variant: stale RFQ hits Ignore(BindComplete) — same mismatch for any Ignore slot.
    #[test]
    fn test_stale_rfq_hits_ignore_bindcomplete() {
        let mut state = ProtocolState::default();
        // Both Parse and Bind are injected (Describe path).
        state.add_ignore('1'); // ParseComplete — injected
        state.add_ignore('2'); // BindComplete  — injected
        state.add('T'); // RowDescription
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        // ParseComplete arrives normally and is swallowed.
        assert_eq!(state.action('1').unwrap(), Action::Ignore);

        // Queue front is now Ignore(BindComplete).
        // A stale RFQ arrives before BindComplete → ProtocolOutOfSync.
        assert!(
            state.action('Z').is_err(),
            "stale RFQ against Ignore(BindComplete) must produce ProtocolOutOfSync"
        );
    }

    // Happy path: injected ParseComplete arrives in order — silently ignored, rest forwarded.
    #[test]
    fn test_injected_parse_happy_path() {
        let mut state = ProtocolState::default();
        state.add_ignore('1'); // ParseComplete — injected, swallowed
        state.add('2'); // BindComplete
        state.add('C'); // CommandComplete
        state.add('Z'); // ReadyForQuery

        assert_eq!(state.action('1').unwrap(), Action::Ignore); // swallowed
        assert_eq!(state.action('2').unwrap(), Action::Forward); // forwarded
        assert_eq!(state.action('C').unwrap(), Action::Forward); // forwarded
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // forwarded
        assert!(state.is_empty());
    }

    // Replicates the full lifecycle of an injected PREPARE that errors:
    //
    // Client sends:  PREPARE foo AS ...  (simple-query style)
    //                EXECUTE (via Query)
    //
    // pgdog injects ahead of the client's Query:
    //   add_ignore('C')  — CommandComplete from PREPARE
    //   add_ignore('Z')  — RFQ from PREPARE
    // Then the client's Query adds:
    //   add('Z')         — the client-visible RFQ
    //
    // Queue before first error: [Ignore(C), Ignore(Z), Code(Z)]
    //
    // Server responds to PREPARE with an error:
    //   'E'  → error branch fires: drain [Ignore(C), Ignore(Z)], count 1 Ignore(RFQ),
    //          push_front loop produces [Ignore(RFQ), Ignore(Error), Code(Z)].
    //          Action::Forward — client receives this error.
    //   'Z'  → matches Ignore(RFQ) → Action::Ignore (PREPARE's RFQ suppressed)
    //
    // Server responds to EXECUTE (which fails because PREPARE never succeeded):
    //   'E'  → fast-path: front is Ignore(Error) → pop → Action::Ignore (suppressed)
    //   'Z'  → Code(Z) → Action::Forward — client receives the closing RFQ
    #[test]
    fn test_injected_prepare_error_full_lifecycle() {
        let mut state = ProtocolState::default();

        // --- setup: replicate what prepared_statements.rs does ---
        // ProtocolMessage::Prepare injects:
        state.add_ignore('C'); // Ignore(CommandComplete) — PREPARE response
        state.add_ignore('Z'); // Ignore(RFQ)            — PREPARE response
                               // ProtocolMessage::Query (client EXECUTE) adds:
        state.add('Z'); // Code(RFQ) — client-visible

        // --- server sends Error for PREPARE ---
        // Error branch: drains [Ignore(C), Ignore(Z)], finds 1 Ignore(Z),
        // rebuilds queue as [Ignore(RFQ), Ignore(Error), Code(Z)].
        assert_eq!(state.action('E').unwrap(), Action::Forward);

        // --- server sends RFQ for PREPARE (now suppressed) ---
        assert_eq!(state.action('Z').unwrap(), Action::Ignore);

        // --- server sends Error for EXECUTE (prepare never succeeded) ---
        // Fast-path: Ignore(Error) is at front → pop and ignore.
        assert_eq!(state.action('E').unwrap(), Action::Ignore);

        // --- server sends RFQ for EXECUTE ---
        // Code(Z) is at front → forwarded to client.
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert!(state.is_empty());
    }

    // =========================================
    // Pipeline multi-sync tests
    // =========================================

    // In pipeline mode, each request runs in isolation; a failure in one must not affect the others.
    #[test]
    fn test_pipeline_multi_sync_error_in_seq1_does_not_affect_seq2_seq3() {
        // Setup: seq1 expects ParseComplete, BindComplete, CommandComplete, ReadyForQuery.
        let mut seq1 = ProtocolState::default();
        seq1.add('1'); // ParseComplete
        seq1.add('2'); // BindComplete
        seq1.add('C'); // CommandComplete (won't arrive — execute errors)
        seq1.add('Z'); // ReadyForQuery

        // Seq1: fails — seq2 and seq3 must still respond.
        assert_eq!(seq1.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(seq1.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(seq1.action('E').unwrap(), Action::Forward); // ErrorResponse
        assert!(seq1.out_of_sync);
        assert_eq!(seq1.len(), 1);
        assert_eq!(seq1.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(!seq1.out_of_sync);
        assert!(seq1.is_empty());

        // Seq2: independent state; seq1's error must not affect it.
        let mut seq2 = ProtocolState::default();
        seq2.add('1');
        seq2.add('2');
        seq2.add('C');
        seq2.add('Z');

        assert_eq!(seq2.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(seq2.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(seq2.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(seq2.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(seq2.is_empty());

        // Seq3: independent state; seq1's error must not affect it.
        let mut seq3 = ProtocolState::default();
        seq3.add('1');
        seq3.add('2');
        seq3.add('C');
        seq3.add('Z');

        assert_eq!(seq3.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(seq3.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(seq3.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(seq3.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(seq3.is_empty());
    }

    // In pipeline mode, a failed request must not prevent subsequent pipelined
    // requests from being processed. Seq1 errors; seq2 and seq3 must still
    // receive their responses.
    //
    // Currently FAILS: when seq1 errors, seq2 and seq3 receive errors instead
    // of their expected responses.
    #[test]
    fn test_pipeline_single_queue_error_only_clears_failing_sync_group() {
        let mut state = ProtocolState::default();
        // Setup: all three sync groups loaded into a single shared queue.
        state.add('1');
        state.add('2');
        state.add('C');
        state.add('Z'); // seq1
        state.add('1');
        state.add('2');
        state.add('C');
        state.add('Z'); // seq2
        state.add('1');
        state.add('2');
        state.add('C');
        state.add('Z'); // seq3

        // Seq1: fails — seq2 and seq3 must still be reachable.
        assert_eq!(state.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(state.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(state.action('E').unwrap(), Action::Forward); // error — must not clear seq2/seq3
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery

        // Seq2: must process normally.
        assert_eq!(state.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(state.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(state.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery

        // Seq3: must process normally.
        assert_eq!(state.action('1').unwrap(), Action::Forward); // ParseComplete
        assert_eq!(state.action('2').unwrap(), Action::Forward); // BindComplete
        assert_eq!(state.action('C').unwrap(), Action::Forward); // CommandComplete
        assert_eq!(state.action('Z').unwrap(), Action::Forward); // ReadyForQuery
        assert!(state.is_empty());
    }
}
