//! Rerwrite messages if using prepared statements.
use crate::net::{
    Close, ProtocolMessage,
    messages::{Bind, Describe, Parse},
};

use super::{Error, PreparedStatements};

/// Rewrite messages.
#[derive(Debug)]
pub struct Rewrite<'a> {
    statements: &'a mut PreparedStatements,
}

impl<'a> Rewrite<'a> {
    /// New rewrite module.
    pub fn new(statements: &'a mut PreparedStatements) -> Self {
        Self { statements }
    }

    /// Rewrite a message if needed.
    pub fn rewrite(&mut self, message: &mut ProtocolMessage) -> Result<(), Error> {
        match message {
            ProtocolMessage::Bind(bind) => Ok(self.bind(bind)?),
            ProtocolMessage::Describe(describe) => Ok(self.describe(describe)?),
            ProtocolMessage::Parse(parse) => Ok(self.parse(parse)?),
            ProtocolMessage::Close(close) => Ok(self.close(close)?),
            _ => Ok(()),
        }
    }

    /// Rewrite Parse message.
    fn parse(&mut self, parse: &mut Parse) -> Result<(), Error> {
        self.statements.insert(parse);
        Ok(())
    }

    /// Rerwrite Bind message.
    fn bind(&mut self, bind: &mut Bind) -> Result<(), Error> {
        let name = self.statements.name(bind.statement());
        if let Some(name) = name {
            bind.rename(name);
        }

        Ok(())
    }

    /// Rewrite Describe message.
    fn describe(&mut self, describe: &mut Describe) -> Result<(), Error> {
        if describe.is_portal() {
            Ok(())
        } else {
            let name = self.statements.name(describe.statement());
            if let Some(name) = name {
                describe.rename(name);
            }
            Ok(())
        }
    }

    /// Handle Close message.
    ///
    /// Portals have their own name space, so a portal Close can carry the
    /// name of a prepared statement we still need.
    fn close(&mut self, close: &Close) -> Result<(), Error> {
        if close.is_statement() {
            self.statements.close(close.name());
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::*;

    use super::*;

    #[test]
    fn test_rewrite() {
        // Don't reuse global one for tests.
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);
        let parse = Parse::named("__sqlx_1", "SELECT * FROM users");
        let mut parse = ProtocolMessage::from(parse);
        rewrite.rewrite(&mut parse).unwrap();
        let parse = Parse::from_bytes(parse.to_bytes()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");

        let bind = Bind::new_statement("__sqlx_1");
        let mut bind_msg = ProtocolMessage::from(bind);
        rewrite.rewrite(&mut bind_msg).unwrap();
        let bind = Bind::from_bytes(bind_msg.to_bytes()).unwrap();
        assert_eq!(bind.statement(), "__pgdog_1");

        let describe = Describe::new_statement("__sqlx_1");
        let mut describe = ProtocolMessage::from(describe);
        rewrite.rewrite(&mut describe).unwrap();
        let describe = Describe::from_bytes(describe.to_bytes()).unwrap();
        assert_eq!(describe.statement(), "__pgdog_1");
        assert_eq!(describe.kind(), 'S');

        assert_eq!(statements.num_statements(), 1);
        assert_eq!(statements.global.read().len(), 1);
    }

    #[test]
    fn test_rewrite_anonymous() {
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);

        let parse = Parse::new_anonymous("SELECT * FROM users");
        let mut parse = ProtocolMessage::from(parse);
        rewrite.rewrite(&mut parse).unwrap();
        let parse = Parse::from_bytes(parse.to_bytes()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.query(), "SELECT * FROM users");

        assert_eq!(statements.num_statements(), 1);
        assert_eq!(statements.global.read().len(), 1);
    }

    /// A portal Close can carry a prepared statement's name. If it releases
    /// the statement, the next sweep takes it while the client still needs it.
    #[test]
    fn test_rewrite_close_portal_keeps_statement() {
        let mut statements = PreparedStatements::default();
        let global = statements.global.clone();

        let mut parse = ProtocolMessage::from(Parse::named("foo", "SELECT $1"));
        statements.maybe_rewrite(&mut parse).unwrap();

        let mut close = ProtocolMessage::from(Close::portal("foo"));
        statements.maybe_rewrite(&mut close).unwrap();

        assert_eq!(global.write().close_unused(0), 0);
        assert_eq!(global.read().len(), 1);
        assert_eq!(
            statements.name("foo").map(|name| name.as_str()),
            Some("__pgdog_1")
        );

        let mut close = ProtocolMessage::from(Close::named("foo"));
        statements.maybe_rewrite(&mut close).unwrap();

        assert_eq!(global.write().close_unused(0), 1);
        assert_eq!(global.read().len(), 0);
    }
}
