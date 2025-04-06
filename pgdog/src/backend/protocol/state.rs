use crate::net::Message;

use super::super::Error;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Forward,
    Ignore,
    ForwardAndRemove(VecDeque<String>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ExecutionCode {
    ReadyForQuery,
    CommandComplete,
    ParseComplete,
    BindComplete,
    CloseComplete,
    DescriptionOrNothing,
    Error,
    Untracked,
    EmptyQueryResponse,
}

impl From<char> for ExecutionCode {
    fn from(value: char) -> Self {
        match value {
            'Z' => Self::ReadyForQuery,
            'C' => Self::CommandComplete,
            '1' => Self::ParseComplete,
            '2' => Self::BindComplete,
            '3' => Self::CloseComplete,
            'T' | 'n' | 't' => Self::DescriptionOrNothing,
            'E' => Self::Error,
            'I' => Self::EmptyQueryResponse,
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
    simple: bool,
}

impl ProtocolState {
    /// Add a message to the ignore list.
    pub fn add_ignore(&mut self, code: impl Into<ExecutionCode>, name: &str) {
        self.queue.push_back(ExecutionItem::Ignore(code.into()));
        self.names.push_back(name.to_owned());
    }

    pub fn add(&mut self, code: impl Into<ExecutionCode>) {
        self.queue.push_back(ExecutionItem::Code(code.into()));
    }

    pub fn add_simulated(&mut self, message: Message) {
        self.simulated.push_back(message);
    }

    pub fn get_simulated(&mut self) -> Option<Message> {
        self.simulated.pop_front()
    }

    /// Should we ignore the message we just received
    /// and not forward it to the client.
    pub fn action(&mut self, code: impl Into<ExecutionCode>) -> Result<Action, Error> {
        let code = code.into();
        match code {
            ExecutionCode::Untracked => return Ok(Action::Forward),
            ExecutionCode::Error => {
                // Remove everything from the execution queue.
                // If client sent a Sync or Query, we will still answer
                // with ReadyForQuery.
                let last = self.queue.pop_back();
                self.queue.clear();
                if let Some(ExecutionItem::Code(ExecutionCode::ReadyForQuery)) = last {
                    self.queue
                        .push_back(ExecutionItem::Code(ExecutionCode::ReadyForQuery));
                }
                return Ok(Action::Forward);
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

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn queue(&self) -> &VecDeque<ExecutionItem> {
        &self.queue
    }

    pub fn set_simple(&mut self) {
        self.simple = true;
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
