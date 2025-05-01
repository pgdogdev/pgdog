//! Rerwrite messages if using prepared statements.
use crate::{
    backend::ProtocolMessage,
    net::messages::{Bind, Describe, Parse},
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
    pub fn rewrite(&mut self, message: ProtocolMessage) -> Result<ProtocolMessage, Error> {
        match message {
            ProtocolMessage::Bind(bind) => Ok(self.bind(bind)?.into()),
            ProtocolMessage::Describe(describe) => Ok(self.describe(describe)?.into()),
            ProtocolMessage::Parse(parse) => Ok(self.parse(parse)?.into()),
            _ => Ok(message),
        }
    }

    /// Rewrite Parse message.
    fn parse(&mut self, parse: Parse) -> Result<Parse, Error> {
        if parse.anonymous() {
            Ok(parse)
        } else {
            let parse = self.statements.insert(parse);
            Ok(parse)
        }
    }

    /// Rerwrite Bind message.
    fn bind(&mut self, bind: Bind) -> Result<Bind, Error> {
        if bind.anonymous() {
            Ok(bind)
        } else {
            let name = self.statements.name(bind.statement());
            if let Some(name) = name {
                Ok(bind.rename(name))
            } else {
                Ok(bind)
            }
        }
    }

    /// Rewrite Describe message.
    fn describe(&mut self, describe: Describe) -> Result<Describe, Error> {
        if describe.anonymous() {
            Ok(describe)
        } else {
            let name = self.statements.name(describe.statement());
            if let Some(name) = name {
                Ok(describe.rename(name))
            } else {
                Ok(describe)
            }
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
        let parse =
            Parse::from_bytes(rewrite.rewrite(parse.into()).unwrap().to_bytes().unwrap()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.name(), "__pgdog_1");
        assert_eq!(parse.query(), "SELECT * FROM users");

        let bind = Bind::test_statement("__sqlx_1");

        let bind =
            Bind::from_bytes(rewrite.rewrite(bind.into()).unwrap().to_bytes().unwrap()).unwrap();
        assert_eq!(bind.statement(), "__pgdog_1");

        let describe = Describe::new_statement("__sqlx_1");

        let describe = Describe::from_bytes(
            rewrite
                .rewrite(describe.into())
                .unwrap()
                .to_bytes()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(describe.statement(), "__pgdog_1");
        assert_eq!(describe.kind(), 'S');

        assert_eq!(statements.len(), 1);
        assert_eq!(statements.global.lock().len(), 1);
    }

    #[test]
    fn test_rewrite_anonymous() {
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);

        let parse = Parse::new_anonymous("SELECT * FROM users");
        let parse =
            Parse::from_bytes(rewrite.rewrite(parse.into()).unwrap().to_bytes().unwrap()).unwrap();

        assert!(parse.anonymous());
        assert_eq!(parse.query(), "SELECT * FROM users");

        assert!(statements.is_empty());
        assert!(statements.global.lock().is_empty());
    }
}
