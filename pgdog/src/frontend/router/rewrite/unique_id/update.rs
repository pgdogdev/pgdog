//! UPDATE statement rewriter for unique_id.

use pg_query::{protobuf::UpdateStmt, NodeEnum};

use super::{
    super::{Context, Error, RewriteModule},
    bigint_const, bigint_param, max_param_number,
};
use crate::{frontend::router::parser::Value, net::Datum, unique_id};

#[derive(Default)]
pub struct UpdateUniqueIdRewrite {}

impl UpdateUniqueIdRewrite {
    pub fn needs_rewrite(stmt: &UpdateStmt) -> bool {
        for target in &stmt.target_list {
            if let Some(NodeEnum::ResTarget(res)) = target.node.as_ref() {
                if let Some(val) = &res.val {
                    if let Ok(Value::Function(ref func)) = Value::try_from(&val.node) {
                        if *func == "pgdog.unique_id" {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    pub fn rewrite_update(
        stmt: &mut UpdateStmt,
        bind: &mut Option<crate::net::Bind>,
        extended: bool,
        param_counter: &mut i32,
    ) -> Result<(), Error> {
        for target in stmt.target_list.iter_mut() {
            if let Some(NodeEnum::ResTarget(ref mut res)) = target.node {
                if let Some(ref mut val) = res.val {
                    if let Ok(Value::Function(name)) = Value::try_from(&val.node) {
                        if name == "pgdog.unique_id" {
                            let id = unique_id::UniqueId::generator()?.next_id();

                            let node = if extended {
                                *param_counter += 1;
                                if let Some(ref mut bind) = bind {
                                    let count = bind.add_parameter(Datum::Bigint(id))?;
                                    if count != *param_counter {
                                        return Err(Error::ParameterCountMismatch);
                                    }
                                }
                                bigint_param(*param_counter)
                            } else {
                                bigint_const(id)
                            };

                            val.node = Some(node);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl RewriteModule for UpdateUniqueIdRewrite {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        let need_rewrite = if let Some(NodeEnum::UpdateStmt(stmt)) = input
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
        let mut param_counter = max_param_number(input.parse_result());

        if let Some(NodeEnum::UpdateStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            Self::rewrite_update(stmt, &mut bind, extended, &mut param_counter)?;
        }

        input.bind_put(bind);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::{bind::Parameter, Bind};
    use std::env::set_var;

    #[test]
    fn test_unique_id_update() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt =
            pg_query::parse(r#"UPDATE omnisharded SET id = pgdog.unique_id() WHERE old_id = 123"#)
                .unwrap()
                .protobuf;
        let mut update = UpdateUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None, None);
        update.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(!output.query().unwrap().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_unique_id_update_parse() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(
            r#"UPDATE omnisharded SET id = pgdog.unique_id(), settings = $1 WHERE old_id = $2"#,
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
                    len: 3,
                    data: "123".into(),
                },
            ],
        );
        let mut input = Context::new(&stmt, Some(&bind), None);
        UpdateUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "UPDATE omnisharded SET id = $3::bigint, settings = $1 WHERE old_id = $2"
        );
    }
}
