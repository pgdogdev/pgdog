use crate::{
    frontend::{Buffer, Error, PreparedStatements},
    net::{Close, CloseComplete, FromBytes, Message, Parameters, Protocol, ReadyForQuery, ToBytes},
};

pub mod action;
pub mod intercept;

pub use action::Action;

/// Query execution engine.
pub struct Engine<'a> {
    prepared_statements: &'a mut PreparedStatements,
    #[allow(dead_code)]
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

    fn intercept(&mut self, buffer: &Buffer) -> Result<Vec<Message>, Error> {
        let only_sync = buffer.iter().all(|m| m.code() == 'S');
        let only_close = buffer.iter().all(|m| ['C', 'S'].contains(&m.code())) && !only_sync;
        let mut messages = vec![];
        for msg in buffer.iter() {
            match msg.code() {
                'C' => {
                    let close = Close::from_bytes(msg.to_bytes()?)?;
                    if close.is_statement() && self.prepared_statements.enforcing_capacity() {
                        self.prepared_statements.close(close.name());
                    }
                    if only_close {
                        messages.push(CloseComplete.message()?)
                    }
                }
                'S' => {
                    if only_close {
                        messages.push(ReadyForQuery::in_transaction(self.in_transaction).message()?)
                    }
                }
                c => {
                    if only_close {
                        return Err(Error::UnexpectedMessage(c)); // Impossible.
                    }
                }
            }
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod test {
    use crate::net::{Parse, Sync};

    use super::*;

    #[tokio::test]
    async fn test_close_prepared() {
        let mut prepared = PreparedStatements::default();
        prepared.enforcing_capacity = true;
        let _renamed = prepared.insert(Parse::named("test", "SELECT $1"));

        assert_eq!(prepared.local.len(), 1);

        let params = Parameters::default();

        let mut engine = Engine::new(&mut prepared, &params, false);

        let buf = Buffer::from(vec![
            Close::named("test").into(),
            Parse::named("whatever", "SELECT $2").into(),
            Sync.into(),
        ]);
        let res = engine.execute(&buf).await.unwrap();

        assert!(matches!(res, Action::Forward));
        assert!(prepared.local.is_empty());
    }
}
