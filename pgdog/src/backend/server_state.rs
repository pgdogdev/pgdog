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

    pub fn process(&mut self, message_code: char) {
        if message_code == 'E' {
            // Clear state when we get error
            self.set_state(&[]);
        }

        if self.active_state_index + 1 > self.states.len() {
            return;
        }

        let current_state = &self.states[self.active_state_index];
        match current_state {
            State::RunningParse => {
                if message_code == '1' {
                    self.active_state_index += 1;
                    return;
                }
                panic!("Received unexpected message {}", message_code)
            }
            State::RunningBind => {
                if message_code == '2' {
                    self.active_state_index += 1;
                    return;
                }
                panic!("Received unexpected message {}", message_code)
            }
            State::RunningClose => {
                if message_code == '3' {
                    self.active_state_index += 1;
                    return;
                }
                panic!("Received unexpected message {}", message_code)
            }
            State::RunningDescribe => match message_code {
                'T' | 'n' => self.active_state_index += 1,
                't' => (),
                _ => panic!("Received unexpected message {}", message_code),
            },
            State::RunningExecute => match message_code {
                'C' | 'I' | 's' => self.active_state_index += 1,
                'D' | 'N' => (),
                _ => panic!("Received unexpected message {}", message_code),
            },
            State::RunningFlush => (),
            State::RunningSync => match message_code {
                'Z' => self.active_state_index += 1,
                _ => panic!("Received unexpected message {}", message_code),
            },
            State::RunningQuery => match message_code {
                'Z' => self.active_state_index += 1,
                _ => panic!("Received unexpected message {}", message_code),
            },
            State::RunningCopy => match message_code {
                'C' => self.active_state_index += 1,
                'E' => (),
            },
            State::RunningCopyFail => {
                // Backend will respond with an E,
                // so nothing to do.
            }
            State::RunningCopyDone => {
                match message_code {
                    'Z' => self.active_state_index += 1,
                    _ => {
                        // TODO: panic on unexpected
                    }
                }
            }
            State::RunningFastpath => {
                match message_code {
                    'Z' => self.active_state_index += 1,
                    _ => {
                        // TODO: panic on unexpected
                    }
                }
            }
            State::RunningPrepare => {
                match message_code {
                    'Z' => self.active_state_index += 1,
                    _ => {
                        // TODO: panic on unexpected
                    }
                }
            }
        }
    }

    pub fn get_successfully_processed(&self) -> usize {
        self.active_state_index
    }

    pub fn get_current_query(&self) -> &[ProtocolMessage] {
        &self.current_request_messages
    }
}
