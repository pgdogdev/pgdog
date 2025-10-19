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
                // Remove everything from the execution queue.
                // The connection is out of sync until client re-syncs it.
                if self.extended {
                    self.out_of_sync = true;
                }
                let last = self.queue.pop_back();
                self.queue.clear();
                if let Some(ExecutionItem::Code(ExecutionCode::ReadyForQuery)) = last {
                    self.queue
                        .push_back(ExecutionItem::Code(ExecutionCode::ReadyForQuery));
                }
                return Ok(Action::Forward);
            }

            ExecutionCode::ReadyForQuery => {
                self.out_of_sync = false;
            }
            _ => (),
        };
        let in_queue = self.queue.pop_front().ok_or(Error::ProtocolOutOfSync)?;
        match in_queue {
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
                    Err(Error::ProtocolOutOfSync)
                }
            }
        }
    }

    pub(crate) fn copy_mode(&self) -> bool {
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
        assert!(state.extended);
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
        assert!(state.copy_mode());
        assert_eq!(state.action('G').unwrap(), Action::Forward);
        // After consuming 'G', we're no longer in copy mode (it's popped from queue)
        assert!(!state.copy_mode());
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
        assert!(!state.copy_mode());

        state.add('G'); // CopyInResponse
        state.add('C'); // CommandComplete
        assert!(state.copy_mode());

        assert_eq!(state.action('G').unwrap(), Action::Forward);
        assert!(!state.copy_mode()); // No longer at front

        assert_eq!(state.action('C').unwrap(), Action::Forward);
        assert!(!state.copy_mode());
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
}
