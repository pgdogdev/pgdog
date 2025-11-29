use pg_query::{protobuf::ParamRef, NodeEnum};

use super::{
    super::{Error, Input, RewriteModule},
    bigint_const,
};
use crate::{
    frontend::router::parser::{Insert, Value},
    net::Datum,
    unique_id,
};

#[derive(Default)]
pub struct InsertUniqueIdRewrite {}

impl RewriteModule for InsertUniqueIdRewrite {
    /// Handle statement rewrite
    fn rewrite(&mut self, input: &mut Input<'_>) -> Result<(), Error> {
        let mut need_rewrite = false;

        if let Some(NodeEnum::InsertStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            let wrapper = Insert::new(stmt);

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
                return Ok(());
            }
        }

        let mut bind = input.bind_take();

        let select = if let Some(NodeEnum::InsertStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            stmt.select_stmt
                .as_mut()
                .ok_or(Error::ParserError)?
                .node
                .as_mut()
                .ok_or(Error::ParserError)?
        } else {
            return Ok(());
        };

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
                                    bigint_const(id)
                                };

                                column.node = Some(node);
                            }
                        }
                    }
                }
            }
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
        let mut input = Input::new(&stmt, None);
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
        let mut input = Input::new(&stmt, Some(&bind));
        InsertUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "INSERT INTO omnisharded (id, settings) VALUES ($3, $1::jsonb), ($4, $2::jsonb)"
        );
    }
}
