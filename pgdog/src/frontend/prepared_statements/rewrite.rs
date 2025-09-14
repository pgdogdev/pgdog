//! Rerwrite messages if using prepared statements.
use crate::net::{
    messages::{Bind, Describe, Parse},
    ProtocolMessage,
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
            ProtocolMessage::Bind(ref mut bind) => Ok(self.bind(bind)?),
            ProtocolMessage::Describe(ref mut describe) => Ok(self.describe(describe)?),
            ProtocolMessage::Parse(ref mut parse) => Ok(self.parse(parse)?),
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
        let parse = Parse::from_bytes(parse.to_bytes().unwrap()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");

        let bind = Bind::new_statement("__sqlx_1");
        let mut bind_msg = ProtocolMessage::from(bind);
        rewrite.rewrite(&mut bind_msg).unwrap();
        let bind = Bind::from_bytes(bind_msg.to_bytes().unwrap()).unwrap();
        assert_eq!(bind.statement(), "__pgdog_1");

        let describe = Describe::new_statement("__sqlx_1");
        let mut describe = ProtocolMessage::from(describe);
        rewrite.rewrite(&mut describe).unwrap();
        let describe = Describe::from_bytes(describe.to_bytes().unwrap()).unwrap();
        assert_eq!(describe.statement(), "__pgdog_1");
        assert_eq!(describe.kind(), 'S');

        assert_eq!(statements.len_local(), 1);
        assert_eq!(statements.global.lock().len(), 1);
    }

    #[test]
    fn test_rewrite_anonymous() {
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);

        let parse = Parse::new_anonymous("SELECT * FROM users");
        let mut parse = ProtocolMessage::from(parse);
        rewrite.rewrite(&mut parse).unwrap();
        let parse = Parse::from_bytes(parse.to_bytes().unwrap()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.query(), "SELECT * FROM users");

        assert_eq!(statements.len_local(), 1);
        assert_eq!(statements.global.lock().len(), 1);
    }
}
