use tracing::error;

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

    /// Should we ignore the message we just received
    /// and not forward it to the client.
    pub fn action(&mut self, code: impl Into<ExecutionCode>) -> Result<Action, Error> {
        let code = code.into();
        match code {
            ExecutionCode::Untracked => return Ok(Action::Forward),
            ExecutionCode::Error => {
                while let Some(op) = self.queue.pop_front() {
                    match op {
                        ExecutionItem::Code(code) => {
                            if code == ExecutionCode::ReadyForQuery {
                                self.queue.push_front(ExecutionItem::Code(code));
                                break;
                            }
                        }
                        _ => (),
                    }
                }
                return Ok(Action::Forward);
            }
            _ => (),
        };
        let in_queue = self.queue.pop_front().ok_or(Error::ProtocolOutOfSync)?;
        match in_queue {
            ExecutionItem::Code(in_queue) => {
                if code == in_queue {
                    Ok(Action::Forward)
                } else if matches!(
                    code,
                    ExecutionCode::CommandComplete | ExecutionCode::EmptyQueryResponse
                ) {
                    // Got command complete / empty query early.
                    // This means this statement was probably a SET.
                    match in_queue {
                        ExecutionCode::BindComplete => (),
                        ExecutionCode::ParseComplete => (),
                        ExecutionCode::CloseComplete => (),
                        _ => {
                            let next = self.queue.pop_front().ok_or(Error::ProtocolOutOfSync)?;
                            if next != ExecutionItem::Code(ExecutionCode::CommandComplete) {
                                return Err(Error::ProtocolOutOfSync);
                            }
                        }
                    }
                    Ok(Action::Forward)
                } else {
                    Err(Error::ProtocolOutOfSync)
                }
            }

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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_state() {
        let mut state = ProtocolState::default();
        state.add_ignore('1', "test");
        assert_eq!(state.action('2').unwrap(), Action::Forward);
        assert_eq!(state.action('Z').unwrap(), Action::Forward);
        assert_eq!(state.action('1').unwrap(), Action::Ignore);
        assert_eq!(state.action('1').unwrap(), Action::Forward);
    }
}
