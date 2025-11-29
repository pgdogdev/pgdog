//! Rewrite input and output types.

use pg_query::protobuf::{ParseResult, RawStmt};

use super::Error;
use crate::{
    frontend::PreparedStatements,
    net::{Bind, Parse, Query},
};

#[derive(Debug, Clone)]
pub struct Input<'a> {
    // Most requeries won't require a rewrite.
    // This is a clone-free way to check.
    original: &'a ParseResult,
    // If a rewrite was done, the statement is saved here.
    rewrite: Option<ParseResult>,
    /// Original bind message, if any.
    bind: Option<&'a Bind>,
    /// Bind rewritten.
    rewrite_bind: Option<Bind>,
}

impl<'a> Input<'a> {
    /// Create new input.
    pub fn new(original: &'a ParseResult, bind: Option<&'a Bind>) -> Self {
        Self {
            original,
            bind,
            rewrite: None,
            rewrite_bind: None,
        }
    }

    /// Get the Bind message, if set.
    pub fn bind(&'a self) -> Option<&'a Bind> {
        if let Some(ref rewrite_bind) = self.rewrite_bind {
            Some(rewrite_bind)
        } else {
            self.bind
        }
    }

    /// Take the Bind message for modification.
    /// Don't forget to return it.
    #[must_use]
    pub fn bind_take(&mut self) -> Option<Bind> {
        if self.rewrite_bind.is_none() {
            self.rewrite_bind = self.bind.cloned();
        }

        self.rewrite_bind.take()
    }

    pub fn bind_put(&mut self, bind: Option<Bind>) {
        self.rewrite_bind = bind;
    }

    /// Get the original (or modified) statement.
    pub fn stmt(&'a self) -> Result<&'a RawStmt, Error> {
        let stmt = if let Some(ref rewrite) = self.rewrite {
            rewrite
        } else {
            self.original
        };
        let root = stmt.stmts.first().ok_or(Error::EmptyQuery)?;
        Ok(root)
    }

    /// Get the mutable statement we're rewriting.
    pub fn stmt_mut(&mut self) -> Result<&mut RawStmt, Error> {
        let stmt = if let Some(ref mut rewrite) = self.rewrite {
            rewrite
        } else {
            self.rewrite = Some(self.original.clone());
            self.rewrite.as_mut().unwrap()
        };

        stmt.stmts.first_mut().ok_or(Error::EmptyQuery)
    }

    /// Assemble statement and add it to the global prepared statements cache.
    pub fn build(mut self) -> Result<Output, Error> {
        if self.rewrite.is_none() {
            Ok(Output::NoOp)
        } else {
            let bind = self.rewrite_bind.take();
            let stmt = self.rewrite.take().ok_or(Error::NoRewrite)?.deparse()?;

            if let Some(mut bind) = bind {
                let mut parse = Parse::new_anonymous(stmt);
                if bind.anonymous() {
                    Ok(Output::Extended { parse, bind })
                } else {
                    let (_, name) = PreparedStatements::global().write().insert(&parse);
                    parse.rename_fast(&name);
                    bind.rename(name);
                    Ok(Output::Extended { parse, bind })
                }
            } else {
                Ok(Output::Simple {
                    query: Query::new(stmt),
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Output {
    NoOp,
    Extended { parse: Parse, bind: Bind },
    Simple { query: Query },
    Multi(Vec<Box<Self>>),
}

impl Output {
    /// Get rewritten query, if any.
    pub fn query(&self) -> Result<&str, ()> {
        match self {
            Self::Extended { parse, .. } => Ok(parse.query()),
            Self::Simple { query } => Ok(query.query()),
            _ => Err(()),
        }
    }
}
