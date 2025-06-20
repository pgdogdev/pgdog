use crate::{
    frontend::{Buffer, Error, PreparedStatements},
    net::{CloseComplete, Message, Parameters, Protocol, ReadyForQuery},
};

pub mod action;
pub mod intercept;

pub use action::Action;

/// Query execution engine.
#[allow(dead_code)]
pub struct Engine<'a> {
    prepared_statements: &'a mut PreparedStatements,
    params: &'a Parameters,
    in_transaction: bool,
}

impl<'a> Engine<'a> {
    pub(crate) fn new(
        prepared_statements: &'a mut PreparedStatements,
        params: &'a Parameters,
        in_transaction: bool,
    ) -> Self {
        Self {
            prepared_statements,
            params,
            in_transaction,
        }
    }

    pub(crate) async fn execute(&mut self, buffer: &Buffer) -> Result<Action, Error> {
        let intercept = self.intercept(buffer)?;
        if !intercept.is_empty() {
            Ok(Action::Intercept(intercept))
        } else {
            Ok(Action::Forward)
        }
    }

    fn intercept(&self, buffer: &Buffer) -> Result<Vec<Message>, Error> {
        let only_close = buffer.iter().all(|m| ['C', 'S'].contains(&m.code()));
        if only_close {
            let mut messages = buffer
                .iter()
                .filter(|m| m.code() == 'C')
                .map(|_| CloseComplete.message())
                .collect::<Result<Vec<Message>, crate::net::Error>>()?;
            if buffer.last().map(|m| m.code() == 'S').unwrap_or(false) {
                messages.push(ReadyForQuery::in_transaction(self.in_transaction).message()?);
            }

            Ok(messages)
        } else {
            Ok(vec![])
        }
    }
}
