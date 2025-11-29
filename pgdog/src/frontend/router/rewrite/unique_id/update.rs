//! UPDATE statement rewriter for unique_id.

use pg_query::{protobuf::ParamRef, NodeEnum};

use super::{
    super::{Error, Input, RewriteModule},
    bigint_const,
};
use crate::{frontend::router::parser::Value, net::Datum, unique_id};

#[derive(Default)]
pub struct UpdateUniqueIdRewrite {}

impl RewriteModule for UpdateUniqueIdRewrite {
    fn rewrite(&mut self, input: &mut Input<'_>) -> Result<(), Error> {
        let mut need_rewrite = false;

        if let Some(NodeEnum::UpdateStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            for target in &stmt.target_list {
                if let Some(NodeEnum::ResTarget(res)) = target.node.as_ref() {
                    if let Some(val) = &res.val {
                        if let Ok(Value::Function(ref func)) = Value::try_from(&val.node) {
                            if *func == "pgdog.unique_id" {
                                need_rewrite = true;
                                break;
                            }
                        }
                    }
                }
            }

            if !need_rewrite {
                return Ok(());
            }
        }

        let mut bind = input.bind_take();

        if let Some(NodeEnum::UpdateStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            for target in stmt.target_list.iter_mut() {
                if let Some(NodeEnum::ResTarget(ref mut res)) = target.node {
                    if let Some(ref mut val) = res.val {
                        if let Ok(Value::Function(name)) = Value::try_from(&val.node) {
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

                                val.node = Some(node);
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
        let mut input = Input::new(&stmt, None);
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
        let mut input = Input::new(&stmt, Some(&bind));
        UpdateUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "UPDATE omnisharded SET id = $3, settings = $1 WHERE old_id = $2"
        );
    }
}
