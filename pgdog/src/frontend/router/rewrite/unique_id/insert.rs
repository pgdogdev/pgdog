use pg_query::{protobuf::InsertStmt, NodeEnum};

use super::{
    super::{Context, Error, RewriteModule},
    bigint_const, bigint_param,
};
use crate::{
    frontend::router::parser::{Insert, Value},
    net::Datum,
    unique_id,
};

#[derive(Default)]
pub struct InsertUniqueIdRewrite {}

impl InsertUniqueIdRewrite {
    pub fn needs_rewrite(stmt: &InsertStmt) -> bool {
        let wrapper = Insert::new(stmt);

        for tuple in wrapper.tuples() {
            for value in tuple.values {
                if let Value::Function(ref func) = value {
                    if *func == "pgdog.unique_id" {
                        return true;
                    }
                }
            }
        }

        false
    }

    pub fn rewrite_insert(
        stmt: &mut InsertStmt,
        bind: &mut Option<crate::net::Bind>,
        extended: bool,
    ) -> Result<(), Error> {
        let mut param_counter = Self::param_count(stmt)?;
        let select = stmt
            .select_stmt
            .as_mut()
            .ok_or(Error::ParserError)?
            .node
            .as_mut()
            .ok_or(Error::ParserError)?;

        if let NodeEnum::SelectStmt(select_stmt) = select {
            for tuple in select_stmt.values_lists.iter_mut() {
                if let Some(NodeEnum::List(ref mut tuple)) = tuple.node {
                    for column in tuple.items.iter_mut() {
                        if let Ok(Value::Function(name)) = Value::try_from(&column.node) {
                            if name == "pgdog.unique_id" {
                                let id = unique_id::UniqueId::generator()?.next_id();

                                let node = if extended {
                                    param_counter += 1;
                                    if let Some(ref mut bind) = bind {
                                        let count = bind.add_parameter(Datum::Bigint(id))?;
                                        // The number of parameters in the query doesn't match what's in the bind message.
                                        if count != param_counter {
                                            return Err(Error::ParameterCountMismatch);
                                        }
                                    }
                                    bigint_param(param_counter)
                                } else {
                                    bigint_const(id)
                                };

                                column.node = Some(node);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn param_count(stmt: &InsertStmt) -> Result<i32, Error> {
        let mut max = 0;

        let select = stmt
            .select_stmt
            .as_ref()
            .ok_or(Error::ParserError)?
            .node
            .as_ref()
            .ok_or(Error::ParserError)?;

        if let NodeEnum::SelectStmt(stmt) = select {
            for tuple in stmt.values_lists.iter() {
                if let Some(NodeEnum::List(ref tuple)) = tuple.node {
                    for column in tuple.items.iter() {
                        if let Some(NodeEnum::ParamRef(ref param)) = column.node {
                            if param.number > max {
                                max = param.number;
                            }
                        }
                    }
                }
            }
        }

        Ok(max)
    }
}

impl RewriteModule for InsertUniqueIdRewrite {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        let need_rewrite = if let Some(NodeEnum::InsertStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            Self::needs_rewrite(stmt)
        } else {
            false
        };

        if !need_rewrite {
            return Ok(());
        }

        let mut bind = input.bind_take();
        let extended = input.extended();

        if let Some(NodeEnum::InsertStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            Self::rewrite_insert(stmt, &mut bind, extended)?;
        }

        input.bind_put(bind);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::bind::{Bind, Parameter};
    use std::env::set_var;

    #[test]
    fn test_unique_id_insert() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(
            r#"
            INSERT INTO omnisharded (id, settings)
            VALUES
                (pgdog.unique_id(), '{}'::JSONB),
                (pgdog.unique_id(), '{"hello": "world"}'::JSONB)"#,
        )
        .unwrap()
        .protobuf;
        let mut insert = InsertUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None, None);
        insert.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(!query.contains("pgdog.unique_id"));
        assert!(
            query.contains("bigint"),
            "Query should contain bigint cast: {}",
            query
        );
    }

    #[test]
    fn test_unique_id_insert_parse() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(
            r#"
            INSERT INTO omnisharded (id, settings)
            VALUES
                (pgdog.unique_id(), $1::JSONB),
                (pgdog.unique_id(), $2::JSONB)"#,
        )
        .unwrap()
        .protobuf;
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
        let mut input = Context::new(&stmt, Some(&bind), None);
        InsertUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "INSERT INTO omnisharded (id, settings) VALUES ($3::bigint, $1::jsonb), ($4::bigint, $2::jsonb)"
        );
    }
}
