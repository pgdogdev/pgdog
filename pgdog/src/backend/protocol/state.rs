use crate::net::{Message, Protocol};

use super::super::Error;
use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Forward,
    Ignore,
    ForwardAndRemove(VecDeque<String>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ExecutionCode {
    ReadyForQuery,
    ExecutionCompleted,
    ParseComplete,
    BindComplete,
    CloseComplete,
    DescriptionOrNothing,
    Error,
    Untracked,
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

#[derive(Debug, Clone, Default)]
pub struct ProtocolState {
    queue: VecDeque<ExecutionItem>,
    names: VecDeque<String>,
    simulated: VecDeque<Message>,
    extended: bool,
    out_of_sync: bool,
}

impl ProtocolState {
    /// Add a message to the ignore list.
    ///
    /// The server will return this message, but we won't send it to the client.
    /// This is used for preparing statements that the client expects to be there
    /// but the server connection doesn't have yet.
    ///
    pub(crate) fn add_ignore(&mut self, code: impl Into<ExecutionCode>, name: &str) {
        let code = code.into();
        self.extended = self.extended || code.extended();
        self.queue.push_back(ExecutionItem::Ignore(code));
        self.names.push_back(name.to_owned());
    }

    /// Add a message to the execution queue. We expect this message
    /// to be returned by the server.
    pub(crate) fn add(&mut self, code: impl Into<ExecutionCode>) {
        let code = code.into();
        self.extended = self.extended || code.extended();
        self.queue.push_back(ExecutionItem::Code(code))
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
                self.names.pop_front().ok_or(Error::ProtocolOutOfSync)?;
                if code == in_queue {
                    Ok(Action::Ignore)
                } else if code == ExecutionCode::Error {
                    Ok(Action::ForwardAndRemove(std::mem::take(&mut self.names)))
                } else {
                    Err(Error::ProtocolOutOfSync)
                }
            }
        }
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

    #[cfg(test)]
    pub(crate) fn in_sync(&self) -> bool {
        !self.out_of_sync
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_state() {
        let mut state = ProtocolState::default();
        state.add_ignore('1', "test");
        assert_eq!(state.action('1').unwrap(), Action::Ignore);
    }
}
