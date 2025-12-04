use pg_query::{protobuf::InsertStmt, NodeEnum};

use super::{
    super::{Context, Error, RewriteModule},
    bigint_const, bigint_param, max_param_number,
};
use crate::{
    frontend::router::{
        parser::{Insert, Value},
        rewrite::UniqueIdPlan,
    },
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
        extended: bool,
        param_counter: &mut i32,
    ) -> Result<Vec<UniqueIdPlan>, Error> {
        let mut plans = vec![];
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
                                    *param_counter += 1;
                                    plans.push(UniqueIdPlan {
                                        param_ref: *param_counter,
                                    });
                                    bigint_param(*param_counter)
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

        Ok(plans)
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

        let extended = input.extended();
        let mut param_counter = max_param_number(input.parse_result());

        if let Some(NodeEnum::InsertStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            input.plan().unique_ids = Self::rewrite_insert(stmt, extended, &mut param_counter)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::Parse;
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
        let mut input = Context::new(&stmt, None);
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
        let query = r#"
            INSERT INTO omnisharded (id, settings)
            VALUES
                (pgdog.unique_id(), $1::JSONB),
                (pgdog.unique_id(), $2::JSONB)"#;
        let stmt = pg_query::parse(query).unwrap().protobuf;
        let parse = Parse::new_anonymous(query);
        let mut input = Context::new(&stmt, Some(&parse));
        InsertUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "INSERT INTO omnisharded (id, settings) VALUES ($3::bigint, $1::jsonb), ($4::bigint, $2::jsonb)"
        );
        // Verify the rewrite plan has the correct parameters
        let plan = output.plan().unwrap();
        assert_eq!(plan.unique_ids.len(), 2);
        assert_eq!(plan.unique_ids[0].param_ref, 3);
        assert_eq!(plan.unique_ids[1].param_ref, 4);
    }
}
