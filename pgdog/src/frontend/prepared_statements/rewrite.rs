//! Rerwrite messages if using prepared statements.
use crate::net::messages::{Bind, Describe, FromBytes, Message, Parse, Protocol};

use super::{request::PreparedRequest, Error, PreparedStatements};

/// Rewrite messages.
#[derive(Debug)]
pub struct Rewrite<'a> {
    statements: &'a mut PreparedStatements,
    requests: Vec<PreparedRequest>,
}

impl<'a> Rewrite<'a> {
    /// New rewrite module.
    pub fn new(statements: &'a mut PreparedStatements) -> Self {
        Self {
            statements,
            requests: vec![],
        }
    }

    /// Rewrite a message if needed.
    pub fn rewrite(&mut self, message: impl Protocol) -> Result<Message, Error> {
        match message.code() {
            'D' => self.describe(message),
            'P' => self.parse(message),
            'B' => self.bind(message),
            _ => Ok(message.message()?),
        }
    }

    /// Rewrite Parse message.
    fn parse(&mut self, message: impl Protocol) -> Result<Message, Error> {
        let parse = Parse::from_bytes(message.to_bytes()?)?;

        if parse.anonymous() {
            Ok(message.message()?)
        } else {
            let parse = self.statements.insert(parse);
            self.requests.push(PreparedRequest::new(&parse.name, true));
            Ok(parse.message()?)
        }
    }

    /// Rerwrite Bind message.
    fn bind(&mut self, message: impl Protocol) -> Result<Message, Error> {
        let bind = Bind::from_bytes(message.to_bytes()?)?;
        if bind.anonymous() {
            Ok(message.message()?)
        } else {
            let name = self
                .statements
                .name(&bind.statement)
                .ok_or(Error::MissingPreparedStatement(bind.statement.clone()))?;
            self.requests.push(PreparedRequest::new(name, false));
            let bind = bind.rename(name);
            self.requests
                .push(PreparedRequest::Bind { bind: bind.clone() });
            Ok(bind.message()?)
        }
    }

    /// Rewrite Describe message.
    fn describe(&mut self, message: impl Protocol) -> Result<Message, Error> {
        let describe = Describe::from_bytes(message.to_bytes()?)?;
        if describe.anonymous() {
            Ok(message.message()?)
        } else {
            let name = self
                .statements
                .name(&describe.statement)
                .ok_or(Error::MissingPreparedStatement(describe.statement.clone()))?;
            self.requests.push(PreparedRequest::new(name, false));
            self.requests.push(PreparedRequest::new_describe(name));
            Ok(describe.rename(name).message()?)
        }
    }

    /// Consume request.
    pub(super) fn requests(&mut self) -> Vec<PreparedRequest> {
        std::mem::take(&mut self.requests)
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::ToBytes;

    use super::*;

    #[test]
    fn test_rewrite() {
        // Don't reuse global one for tests.
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);
        let parse = Parse::named("__sqlx_1", "SELECT * FROM users");
        let parse = Parse::from_bytes(rewrite.rewrite(parse).unwrap().to_bytes().unwrap()).unwrap();

        assert!(!parse.anonymous());
        assert_eq!(parse.name, "__pgdog_1");
        assert_eq!(parse.query, "SELECT * FROM users");
        let requests = rewrite.requests();
        let request = requests.first().unwrap();
        assert_eq!(request.name(), "__pgdog_1");
        assert!(request.is_new());

        let bind = Bind {
            statement: "__sqlx_1".into(),
            ..Default::default()
        };

        let bind = Bind::from_bytes(rewrite.rewrite(bind).unwrap().to_bytes().unwrap()).unwrap();
        assert_eq!(bind.statement, "__pgdog_1");
        let requests = rewrite.requests();
        let request = requests.first().unwrap();
        assert_eq!(request.name(), "__pgdog_1");
        assert!(!request.is_new());

        let describe = Describe {
            statement: "__sqlx_1".into(),
            kind: 'S',
        };

        let describe =
            Describe::from_bytes(rewrite.rewrite(describe).unwrap().to_bytes().unwrap()).unwrap();
        assert_eq!(describe.statement, "__pgdog_1");
        assert_eq!(describe.kind, 'S');
        let requests = rewrite.requests();
        let request = requests.first().unwrap();
        assert_eq!(request.name(), "__pgdog_1");
        assert!(!request.is_new());

        assert_eq!(statements.len(), 1);
        assert_eq!(statements.global.lock().len(), 1);
    }

    #[test]
    fn test_rewrite_anonymous() {
        let mut statements = PreparedStatements::default();
        let mut rewrite = Rewrite::new(&mut statements);

        let parse = Parse::new_anonymous("SELECT * FROM users");
        let parse = Parse::from_bytes(rewrite.rewrite(parse).unwrap().to_bytes().unwrap()).unwrap();

        assert!(parse.anonymous());
        assert_eq!(parse.query, "SELECT * FROM users");

        assert!(statements.is_empty());
        assert!(statements.global.lock().is_empty());
    }
}
