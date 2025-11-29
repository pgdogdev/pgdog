use pg_query::{
    protobuf::{a_const::Val, AConst, InsertStmt, ParamRef},
    Node, NodeEnum,
};

use super::super::Error;
use crate::{
    frontend::{
        router::parser::{Insert, Value},
        PreparedStatements,
    },
    net::{Bind, Datum, Parse, Query},
    unique_id,
};

pub struct InsertUniqueIdRewrite<'a> {
    stmt: &'a InsertStmt,
    bind: Option<&'a Bind>,
}

#[derive(Debug, Clone)]
pub enum InsertRewriteResult {
    Extended { parse: Parse, bind: Bind },
    Simple { query: Query },
}

#[derive(Debug, Clone)]
pub struct InsertRewriteOutput {
    /// Rewritten AST. This should be used for any subsequent operations.
    pub stmt: InsertStmt,
    /// Rewritten Query or Parse and Bind.
    pub rewrite: InsertRewriteResult,
}

impl InsertRewriteOutput {
    /// Get query text.
    pub fn query(&self) -> &str {
        match &self.rewrite {
            InsertRewriteResult::Extended { parse, .. } => parse.query(),
            InsertRewriteResult::Simple { query } => query.query(),
        }
    }
}

impl<'a> InsertUniqueIdRewrite<'a> {
    /// Create new INSERT statement rewriter
    pub fn new(stmt: &'a InsertStmt, bind: Option<&'a Bind>) -> Self {
        Self { stmt, bind }
    }

    /// Handle statement rewrite
    pub fn rewrite(&self) -> Result<Option<InsertRewriteOutput>, Error> {
        let mut need_rewrite = false;

        let wrapper = Insert::new(self.stmt);

        for tuple in wrapper.tuples() {
            for value in tuple.values {
                if let Value::Function(ref func) = value {
                    if *func == "pgdog.unique_id" {
                        need_rewrite = true;
                    }
                }
            }
        }

        if !need_rewrite {
            return Ok(None);
        }

        let mut bind = self.bind.cloned();
        let mut stmt = self.stmt.clone();

        let select = stmt
            .select_stmt
            .as_mut()
            .ok_or(Error::ParserError)?
            .node
            .as_mut()
            .ok_or(Error::ParserError)?;
        if let NodeEnum::SelectStmt(stmt) = select {
            for tuple in stmt.values_lists.iter_mut() {
                if let Some(NodeEnum::List(ref mut tuple)) = tuple.node {
                    for column in tuple.items.iter_mut() {
                        if let Ok(Value::Function(name)) = Value::try_from(&column.node) {
                            // Replace function call with value.
                            if name == "pgdog.unique_id" {
                                let id = unique_id::UniqueId::generator()?.next_id();

                                let node = if let Some(ref mut bind) = bind {
                                    NodeEnum::ParamRef(ParamRef {
                                        number: bind.add_parameter(Datum::Bigint(id))?,
                                        ..Default::default()
                                    })
                                } else {
                                    NodeEnum::AConst(AConst {
                                        val: Some(Val::Sval(pg_query::protobuf::String {
                                            sval: id.to_string(),
                                        })),
                                        ..Default::default()
                                    })
                                };

                                column.node = Some(node);
                            }
                        }
                    }
                }
            }
        }

        let wrapper = Node {
            node: Some(NodeEnum::InsertStmt(Box::new(stmt.clone()))),
            ..Default::default()
        };

        let rewrite = match bind {
            Some(mut bind) => {
                let mut parse = Parse::new_anonymous(&wrapper.deparse()?);
                if !bind.anonymous() {
                    let (_, name) = PreparedStatements::global().write().insert(&parse);
                    parse.rename_fast(&name);
                    bind.rename(&name);
                }

                InsertRewriteResult::Extended { parse, bind }
            }

            None => InsertRewriteResult::Simple {
                query: Query::new(wrapper.deparse()?),
            },
        };

        Ok(Some(InsertRewriteOutput { stmt, rewrite }))
    }
}

#[cfg(test)]
mod test {

    use std::env::set_var;

    use crate::net::bind::Parameter;

    use super::*;

    fn insert_root(query: &str) -> InsertStmt {
        let stmt = pg_query::parse(query).unwrap();
        let root = stmt
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap()
            .stmt
            .unwrap()
            .node
            .unwrap();
        if let NodeEnum::InsertStmt(stmt) = root {
            *stmt.clone()
        } else {
            panic!("not an insert")
        }
    }

    #[test]
    fn test_unique_id_insert() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = insert_root(
            r#"
            INSERT INTO omnisharded (id, settings)
            VALUES
                (pgdog.unique_id(), '{}'::JSONB),
                (pgdog.unique_id(), '{"hello": "world"}'::JSONB)"#,
        );
        let insert = InsertUniqueIdRewrite::new(&stmt, None);
        let rewrite = insert.rewrite().unwrap().unwrap();
        assert!(!rewrite.query().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_unique_id_insert_parse() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = insert_root(
            r#"
            INSERT INTO omnisharded (id, settings)
            VALUES
                (pgdog.unique_id(), $1::JSONB),
                (pgdog.unique_id(), $2::JSONB)"#,
        );
        let bind = Bind::new_params(
            "",
            &[
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
            ],
        );
        let rewrite = InsertUniqueIdRewrite::new(&stmt, Some(&bind))
            .rewrite()
            .unwrap()
            .unwrap();
        assert_eq!(
            rewrite.query(),
            "INSERT INTO omnisharded (id, settings) VALUES ($3, $1::jsonb), ($4, $2::jsonb)"
        );
    }
}
