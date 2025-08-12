use crate::{
    frontend::Error,
    net::{Close, CloseComplete, FromBytes, Message, Protocol, ReadyForQuery, ToBytes},
};

pub mod action;
pub mod context;

pub use action::Action;
pub use context::EngineContext;

/// Query execution engine.
pub struct Engine<'a> {
    context: EngineContext<'a>,
}

impl<'a> Engine<'a> {
    /// Create new query execution engine.
    pub(crate) fn new(context: EngineContext<'a>) -> Self {
        Self { context }
    }

    /// Execute whatever is currently in the client's buffer.
    pub(crate) async fn execute(&mut self) -> Result<Action, Error> {
        let intercept = self.intercept()?;
        if !intercept.is_empty() {
            Ok(Action::Intercept(intercept))
        } else {
            Ok(Action::Forward)
        }
    }

    /// Intercept queries without disturbing backend servers.
    fn intercept(&mut self) -> Result<Vec<Message>, Error> {
        let only_sync = self.context.buffer.iter().all(|m| m.code() == 'S');
        let only_close = self
            .context
            .buffer
            .iter()
            .all(|m| ['C', 'S'].contains(&m.code()))
            && !only_sync;
        let mut messages = vec![];
        for msg in self.context.buffer.iter() {
            match msg.code() {
                'C' => {
                    let close = Close::from_bytes(msg.to_bytes()?)?;
                    if close.is_statement() {
                        self.context.prepared_statements.close(close.name());
                    }
                    if only_close {
                        messages.push(CloseComplete.message()?)
                    }
                }
                'S' => {
                    if only_close || only_sync && !self.context.connected {
                        messages.push(
                            ReadyForQuery::in_transaction(self.context.in_transaction).message()?,
                        )
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
    use crate::{
        frontend::{ClientRequest, PreparedStatements},
        net::{Parameters, Parse, Sync},
    };

    use super::*;

    #[tokio::test]
    async fn test_close_prepared() {
        let mut prepared = PreparedStatements::default();
        prepared.capacity = 0;
        let _renamed = prepared.insert(Parse::named("test", "SELECT $1"));

        assert_eq!(prepared.local.len(), 1);
        let params = Parameters::default();
        let buf = ClientRequest::from(vec![
            Close::named("test").into(),
            Parse::named("whatever", "SELECT $2").into(),
            Sync.into(),
        ]);

        let context = EngineContext {
            connected: false,
            prepared_statements: &mut prepared,
            params: &params,
            in_transaction: false,
            buffer: &buf,
        };

        let mut engine = Engine::new(context);

        let res = engine.execute().await.unwrap();

        assert!(matches!(res, Action::Forward));
        assert!(prepared.local.is_empty());
    }
}
