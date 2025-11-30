//! SELECT statement rewriter for unique_id.

use pg_query::{
    protobuf::{Node, SelectStmt},
    NodeEnum,
};

use super::{
    super::{Context, Error, RewriteModule},
    bigint_const, bigint_param,
};
use crate::{frontend::router::parser::Value, net::Datum, unique_id};

#[derive(Default)]
pub struct SelectUniqueIdRewrite {}

impl SelectUniqueIdRewrite {
    pub fn needs_rewrite(stmt: &SelectStmt) -> bool {
        // Check target_list (SELECT columns)
        for target in &stmt.target_list {
            if let Some(NodeEnum::ResTarget(res)) = target.node.as_ref() {
                if let Some(val) = &res.val {
                    if let Ok(Value::Function(ref func)) = Value::try_from(&val.node) {
                        if func == "pgdog.unique_id" {
                            return true;
                        }
                    }
                }
            }
        }

        // Check CTEs recursively
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref expr)) = cte.node {
                    if let Some(ref query) = expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref inner)) = query.node {
                            if Self::needs_rewrite(inner) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // Check subqueries in FROM clause
        for from in &stmt.from_clause {
            if Self::needs_rewrite_from_node(from) {
                return true;
            }
        }

        // Check UNION/INTERSECT/EXCEPT (larg/rarg are Box<SelectStmt>)
        if let Some(ref larg) = stmt.larg {
            if Self::needs_rewrite(larg) {
                return true;
            }
        }
        if let Some(ref rarg) = stmt.rarg {
            if Self::needs_rewrite(rarg) {
                return true;
            }
        }

        false
    }

    fn needs_rewrite_from_node(node: &Node) -> bool {
        match node.node.as_ref() {
            Some(NodeEnum::RangeSubselect(subselect)) => {
                if let Some(ref subquery) = subselect.subquery {
                    if let Some(NodeEnum::SelectStmt(ref inner)) = subquery.node {
                        return Self::needs_rewrite(inner);
                    }
                }
                false
            }
            Some(NodeEnum::JoinExpr(join)) => {
                let left = join
                    .larg
                    .as_ref()
                    .is_some_and(|n| Self::needs_rewrite_from_node(n));
                let right = join
                    .rarg
                    .as_ref()
                    .is_some_and(|n| Self::needs_rewrite_from_node(n));
                left || right
            }
            _ => false,
        }
    }

    pub fn rewrite_select(
        stmt: &mut SelectStmt,
        bind: &mut Option<crate::net::Bind>,
    ) -> Result<(), Error> {
        // Rewrite target_list
        for target in stmt.target_list.iter_mut() {
            if let Some(NodeEnum::ResTarget(ref mut res)) = target.node {
                if let Some(ref mut val) = res.val {
                    if let Ok(Value::Function(name)) = Value::try_from(&val.node) {
                        if name == "pgdog.unique_id" {
                            let id = unique_id::UniqueId::generator()?.next_id();

                            let node = if let Some(ref mut bind) = bind {
                                bigint_param(bind.add_parameter(Datum::Bigint(id))?)
                            } else {
                                bigint_const(id)
                            };

                            val.node = Some(node);
                        }
                    }
                }
            }
        }

        // Rewrite CTEs recursively
        if let Some(ref mut with_clause) = stmt.with_clause {
            for cte in with_clause.ctes.iter_mut() {
                if let Some(NodeEnum::CommonTableExpr(ref mut expr)) = cte.node {
                    if let Some(ref mut query) = expr.ctequery {
                        if let Some(NodeEnum::SelectStmt(ref mut inner)) = query.node {
                            Self::rewrite_select(inner, bind)?;
                        }
                    }
                }
            }
        }

        // Rewrite subqueries in FROM clause
        for from in stmt.from_clause.iter_mut() {
            Self::rewrite_from_node(from, bind)?;
        }

        // Rewrite UNION/INTERSECT/EXCEPT (larg/rarg are Box<SelectStmt>)
        if let Some(ref mut larg) = stmt.larg {
            Self::rewrite_select(larg, bind)?;
        }
        if let Some(ref mut rarg) = stmt.rarg {
            Self::rewrite_select(rarg, bind)?;
        }

        Ok(())
    }

    fn rewrite_from_node(
        node: &mut Node,
        bind: &mut Option<crate::net::Bind>,
    ) -> Result<(), Error> {
        match node.node.as_mut() {
            Some(NodeEnum::RangeSubselect(ref mut subselect)) => {
                if let Some(ref mut subquery) = subselect.subquery {
                    if let Some(NodeEnum::SelectStmt(ref mut inner)) = subquery.node {
                        Self::rewrite_select(inner, bind)?;
                    }
                }
            }
            Some(NodeEnum::JoinExpr(ref mut join)) => {
                if let Some(ref mut larg) = join.larg {
                    Self::rewrite_from_node(larg, bind)?;
                }
                if let Some(ref mut rarg) = join.rarg {
                    Self::rewrite_from_node(rarg, bind)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl RewriteModule for SelectUniqueIdRewrite {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        let need_rewrite = if let Some(NodeEnum::SelectStmt(stmt)) = input
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

        if let Some(NodeEnum::SelectStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            Self::rewrite_select(stmt, &mut bind)?;
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
    fn test_unique_id_select() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"SELECT pgdog.unique_id() AS id"#)
            .unwrap()
            .protobuf;
        let mut rewrite = SelectUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        println!("output: {}", output.query().unwrap());
        assert!(!output.query().unwrap().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_unique_id_select_with_bind() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"SELECT pgdog.unique_id() AS id, $1 AS name"#)
            .unwrap()
            .protobuf;
        let bind = Bind::new_params(
            "",
            &[Parameter {
                len: 4,
                data: "test".into(),
            }],
        );
        let mut input = Context::new(&stmt, Some(&bind));
        SelectUniqueIdRewrite::default()
            .rewrite(&mut input)
            .unwrap();
        let output = input.build().unwrap();
        assert_eq!(
            output.query().unwrap(),
            "SELECT $2::bigint AS id, $1 AS name"
        );
    }

    #[test]
    fn test_unique_id_select_cte() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt =
            pg_query::parse(r#"WITH ids AS (SELECT pgdog.unique_id() AS id) SELECT * FROM ids"#)
                .unwrap()
                .protobuf;
        let mut rewrite = SelectUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(!output.query().unwrap().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_unique_id_select_subquery() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"SELECT * FROM (SELECT pgdog.unique_id() AS id) sub"#)
            .unwrap()
            .protobuf;
        let mut rewrite = SelectUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(!output.query().unwrap().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_unique_id_select_union() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(
            r#"SELECT pgdog.unique_id() AS id UNION ALL SELECT pgdog.unique_id() AS id"#,
        )
        .unwrap()
        .protobuf;
        let mut rewrite = SelectUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(!output.query().unwrap().contains("pgdog.unique_id"));
    }

    #[test]
    fn test_no_rewrite_when_no_unique_id() {
        let stmt = pg_query::parse(r#"SELECT id FROM users"#).unwrap().protobuf;
        let mut rewrite = SelectUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(matches!(output, super::super::super::StepOutput::NoOp));
    }
}
