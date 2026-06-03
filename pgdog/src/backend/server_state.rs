use super::Error;
use crate::net::{Protocol, ProtocolMessage};

pub trait StateTransition {
    fn process(self, message_code: char) -> impl StateTransition;
}

#[derive(Debug)]
pub enum State {
    RunningParse,
    RunningBind,
    RunningDescribe,
    RunningExecute,
    RunningClose,
    RunningFlush,
    RunningSync,
    RunningQuery,
    RunningCopy,
    RunningCopyDone,
    RunningCopyFail,
    RunningFastpath,
    RunningPrepare,
}

impl From<&ProtocolMessage> for State {
    fn from(value: &ProtocolMessage) -> Self {
        match value {
            ProtocolMessage::Parse(_) => State::RunningParse,
            ProtocolMessage::Prepare { .. } => State::RunningPrepare,
            ProtocolMessage::Bind(_) => State::RunningBind,
            ProtocolMessage::Describe(_) => State::RunningDescribe,
            ProtocolMessage::Execute(_) => State::RunningExecute,
            ProtocolMessage::Close(_) => State::RunningClose,
            ProtocolMessage::Sync(_) => State::RunningSync,
            ProtocolMessage::Query(_) => State::RunningQuery,
            ProtocolMessage::Other(message) => match message.code() {
                'H' => State::RunningFlush,
                _ => panic!("Unexpected other type {:?}", value.code()),
            },
            ProtocolMessage::CopyData(_) => State::RunningCopy,
            ProtocolMessage::CopyFail(_) => State::RunningCopyFail,
            ProtocolMessage::CopyDone(_) => State::RunningCopyDone,
            ProtocolMessage::Fastpath(_) => State::RunningFastpath,
        }
    }
}

#[derive(Debug)]
pub struct ServerState {
    states: Vec<State>,
    active_state_index: usize,
    current_request_messages: Vec<ProtocolMessage>,
}

impl ServerState {
    pub fn new() -> Self {
        ServerState {
            states: Vec::new(),
            active_state_index: 0,
            current_request_messages: Vec::new(),
        }
    }

    pub fn set_state(&mut self, messages: &[ProtocolMessage]) {
        self.states = messages.iter().map(|msg| msg.into()).collect();
        self.current_request_messages = messages.to_vec();

        self.active_state_index = 0;
    }

    pub fn process(&mut self, message_code: char) -> Result<(), Error> {
        if message_code == 'E' {
            // Clear state when we get error
            self.set_state(&[]);
        }

        if self.active_state_index + 1 > self.states.len() {
            return Ok(());
        }

        let current_state = &self.states[self.active_state_index];
        match current_state {
            State::RunningParse => match message_code {
                '1' => self.active_state_index += 1,
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningBind => match message_code {
                '2' => self.active_state_index += 1,
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningClose => match message_code {
                '3' => self.active_state_index += 1,
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningDescribe => match message_code {
                'T' | 'n' => self.active_state_index += 1,
                't' => (),
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningExecute => match message_code {
                'C' | 'I' | 's' => self.active_state_index += 1,
                'D' | 'N' => (),
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningFlush => (),
            State::RunningSync => match message_code {
                'Z' => self.active_state_index += 1,
                _ => return Err(Error::UnexpectedMessage(message_code)),
            },
            State::RunningQuery => match message_code {
                'Z' => self.active_state_index += 1,
                _ => (),
            },
            State::RunningCopy => match message_code {
                'C' => self.active_state_index += 1,
                _ => (),
            },
            State::RunningCopyFail => {
                // Backend will respond with an E,
                // so nothing to do.
            }
            State::RunningCopyDone => match message_code {
                'Z' => self.active_state_index += 1,
                _ => (),
            },
            State::RunningFastpath => match message_code {
                'Z' => self.active_state_index += 1,
                _ => (),
            },
            State::RunningPrepare => match message_code {
                'Z' => self.active_state_index += 1,
                _ => (),
            },
        }

        Ok(())
    }

    pub fn get_successfully_processed(&self) -> usize {
        self.active_state_index
    }

    pub fn get_current_query(&self) -> &[ProtocolMessage] {
        &self.current_request_messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::{
        Bind, Close, CopyData, CopyDone, CopyFail, Describe, Execute, Fastpath, Flush, FromBytes,
        Parse, Query, Sync,
    };
    use bytes::{BufMut, BytesMut};

    fn make_fastpath() -> ProtocolMessage {
        let mut buf = BytesMut::new();
        buf.put_u8(b'F');
        buf.put_i32(4);
        ProtocolMessage::Fastpath(
            Fastpath::from_bytes(buf.freeze()).expect("fastpath shouldn't fail"),
        )
    }

    #[test]
    fn new_state_starts_empty() {
        let state = ServerState::new();
        assert_eq!(state.get_successfully_processed(), 0);
        assert!(state.get_current_query().is_empty());
    }

    #[test]
    fn set_state_resets_index() {
        let mut state = ServerState::new();
        let messages = vec![
            ProtocolMessage::Query(Query::new("SELECT 1")),
            ProtocolMessage::Sync(Sync::new()),
        ];
        state.set_state(&messages);
        // Advance past the query
        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);

        // Reset with new messages
        state.set_state(&[ProtocolMessage::Query(Query::new("SELECT 2"))]);
        assert_eq!(state.get_successfully_processed(), 0);
    }

    #[test]
    fn set_state_stores_messages() {
        let mut state = ServerState::new();
        let messages = vec![
            ProtocolMessage::Parse(Parse::new_anonymous("SELECT 1")),
            ProtocolMessage::Bind(Bind::default()),
            ProtocolMessage::Execute(Execute::new()),
            ProtocolMessage::Sync(Sync::new()),
        ];
        state.set_state(&messages);
        assert_eq!(state.get_current_query().len(), 4);
    }

    #[test]
    fn state_from_parse() {
        let msg = ProtocolMessage::Parse(Parse::new_anonymous("SELECT 1"));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningParse));
    }

    #[test]
    fn state_from_bind() {
        let msg = ProtocolMessage::Bind(Bind::default());
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningBind));
    }

    #[test]
    fn state_from_describe() {
        let msg = ProtocolMessage::Describe(Describe::new_statement(""));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningDescribe));
    }

    #[test]
    fn state_from_execute() {
        let msg = ProtocolMessage::Execute(Execute::new());
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningExecute));
    }

    #[test]
    fn state_from_close() {
        let msg = ProtocolMessage::Close(Close::named("stmt"));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningClose));
    }

    #[test]
    fn state_from_sync() {
        let msg = ProtocolMessage::Sync(Sync::new());
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningSync));
    }

    #[test]
    fn state_from_query() {
        let msg = ProtocolMessage::Query(Query::new("SELECT 1"));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningQuery));
    }

    #[test]
    fn state_from_copy_data() {
        let msg = ProtocolMessage::CopyData(CopyData::new(b"row data"));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningCopy));
    }

    #[test]
    fn state_from_copy_fail() {
        let msg = ProtocolMessage::CopyFail(CopyFail::new("error"));
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningCopyFail));
    }

    #[test]
    fn state_from_copy_done() {
        let msg = ProtocolMessage::CopyDone(CopyDone);
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningCopyDone));
    }

    #[test]
    fn state_from_flush() {
        let msg = ProtocolMessage::Other(Flush.message().unwrap());
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningFlush));
    }

    #[test]
    fn state_from_fastpath() {
        let msg = make_fastpath();
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningFastpath));
    }

    #[test]
    fn state_from_prepare() {
        let msg = ProtocolMessage::Prepare {
            name: "stmt1".to_string(),
            statement: "SELECT 1".to_string(),
        };
        let state: State = (&msg).into();
        assert!(matches!(state, State::RunningPrepare));
    }

    #[test]
    fn simple_query_advances_on_ready_for_query() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Query(Query::new("SELECT 1"))]);

        // Data rows and other messages don't advance
        state.process('D').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);
        state.process('T').unwrap(); // RowDescription
        assert_eq!(state.get_successfully_processed(), 0);
        state.process('C').unwrap(); // CommandComplete
        assert_eq!(state.get_successfully_processed(), 0);

        // ReadyForQuery advances
        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn extended_query_full_cycle() {
        let mut state = ServerState::new();
        state.set_state(&[
            ProtocolMessage::Parse(Parse::new_anonymous("SELECT $1")),
            ProtocolMessage::Bind(Bind::default()),
            ProtocolMessage::Describe(Describe::new_statement("")),
            ProtocolMessage::Execute(Execute::new()),
            ProtocolMessage::Sync(Sync::new()),
        ]);

        // ParseComplete
        state.process('1').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);

        // BindComplete
        state.process('2').unwrap();
        assert_eq!(state.get_successfully_processed(), 2);

        // RowDescription (from Describe)
        state.process('T').unwrap();
        assert_eq!(state.get_successfully_processed(), 3);

        // DataRow (from Execute) - doesn't advance
        state.process('D').unwrap();
        assert_eq!(state.get_successfully_processed(), 3);

        // CommandComplete (from Execute) - advances
        state.process('C').unwrap();
        assert_eq!(state.get_successfully_processed(), 4);

        // ReadyForQuery (from Sync)
        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 5);
    }

    #[test]
    fn describe_advances_on_no_data() {
        let mut state = ServerState::new();
        state.set_state(&[
            ProtocolMessage::Describe(Describe::new_statement("")),
            ProtocolMessage::Sync(Sync::new()),
        ]);

        // NoData response
        state.process('n').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn describe_ignores_parameter_description() {
        let mut state = ServerState::new();
        state.set_state(&[
            ProtocolMessage::Describe(Describe::new_statement("")),
            ProtocolMessage::Sync(Sync::new()),
        ]);

        // ParameterDescription ('t') doesn't advance
        state.process('t').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);

        // RowDescription advances
        state.process('T').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn execute_advances_on_empty_query() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Execute(Execute::new())]);

        // EmptyQueryResponse
        state.process('I').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn execute_advances_on_portal_suspended() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Execute(Execute::new())]);

        // PortalSuspended
        state.process('s').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn execute_ignores_notice_response() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Execute(Execute::new())]);

        // NoticeResponse doesn't advance
        state.process('N').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);

        // CommandComplete advances
        state.process('C').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn close_advances_on_close_complete() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Close(Close::named("stmt"))]);

        // CloseComplete
        state.process('3').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn error_response_clears_state() {
        let mut state = ServerState::new();
        state.set_state(&[
            ProtocolMessage::Parse(Parse::new_anonymous("SELECT 1")),
            ProtocolMessage::Bind(Bind::default()),
            ProtocolMessage::Sync(Sync::new()),
        ]);

        // ParseComplete
        state.process('1').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);

        // ErrorResponse clears everything
        state.process('E').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);
        assert!(state.get_current_query().is_empty());
    }

    #[test]
    fn copy_data_advances_on_command_complete() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::CopyData(CopyData::new(b"data"))]);

        // Other messages don't advance
        state.process('d').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);

        // CommandComplete advances
        state.process('C').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn copy_done_advances_on_ready_for_query() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::CopyDone(CopyDone)]);

        state.process('C').unwrap(); // CommandComplete doesn't advance
        assert_eq!(state.get_successfully_processed(), 0);

        state.process('Z').unwrap(); // ReadyForQuery advances
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn copy_fail_does_not_advance() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::CopyFail(CopyFail::new("abort"))]);

        // Backend responds with ErrorResponse, but CopyFail state itself doesn't advance
        state.process('E').unwrap();
        // Error clears state entirely
        assert_eq!(state.get_successfully_processed(), 0);
    }

    #[test]
    fn flush_never_advances() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Other(Flush.message().unwrap())]);

        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);
        state.process('T').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);
    }

    #[test]
    fn fastpath_advances_on_ready_for_query() {
        let mut state = ServerState::new();
        state.set_state(&[make_fastpath()]);

        state.process('V').unwrap(); // FunctionCallResponse doesn't advance
        assert_eq!(state.get_successfully_processed(), 0);

        state.process('Z').unwrap(); // ReadyForQuery advances
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn prepare_advances_on_ready_for_query() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Prepare {
            name: "stmt1".to_string(),
            statement: "SELECT 1".to_string(),
        }]);

        state.process('T').unwrap(); // RowDescription doesn't advance
        assert_eq!(state.get_successfully_processed(), 0);

        state.process('Z').unwrap(); // ReadyForQuery advances
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn process_does_nothing_when_all_states_consumed() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Sync(Sync::new())]);

        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);

        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
        state.process('T').unwrap();
        assert_eq!(state.get_successfully_processed(), 1);
    }

    #[test]
    fn process_does_nothing_on_empty_state() {
        let mut state = ServerState::new();
        state.process('Z').unwrap();
        assert_eq!(state.get_successfully_processed(), 0);
    }

    #[test]
    fn parse_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Parse(Parse::new_anonymous("SELECT 1"))]);
        let result = state.process('Z');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }

    #[test]
    fn bind_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Bind(Bind::default())]);
        let result = state.process('Z');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }

    #[test]
    fn close_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Close(Close::named("s"))]);
        let result = state.process('Z');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }

    #[test]
    fn describe_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Describe(Describe::new_statement(""))]);
        let result = state.process('Z');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }

    #[test]
    fn execute_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Execute(Execute::new())]);
        let result = state.process('Z');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }

    #[test]
    fn sync_errors_on_unexpected() {
        let mut state = ServerState::new();
        state.set_state(&[ProtocolMessage::Sync(Sync::new())]);
        let result = state.process('T');
        assert!(matches!(result, Err(Error::UnexpectedMessage(_))));
    }
}
