//! EXPLAIN statement rewriter for unique_id.

use pg_query::NodeEnum;

use super::{
    super::{Context, Error, RewriteModule},
    max_param_number, InsertUniqueIdRewrite, SelectUniqueIdRewrite, UpdateUniqueIdRewrite,
};

#[derive(Default)]
pub struct ExplainUniqueIdRewrite {}

impl RewriteModule for ExplainUniqueIdRewrite {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        // Check if this is an EXPLAIN statement
        let is_explain = matches!(
            input
                .stmt()?
                .stmt
                .as_ref()
                .and_then(|stmt| stmt.node.as_ref()),
            Some(NodeEnum::ExplainStmt(_))
        );

        if !is_explain {
            return Ok(());
        }

        // Get the inner query type and dispatch to appropriate rewriter
        let inner_type = if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            stmt.query
                .as_ref()
                .and_then(|q| q.node.as_ref())
                .map(|n| match n {
                    NodeEnum::SelectStmt(_) => "select",
                    NodeEnum::InsertStmt(_) => "insert",
                    NodeEnum::UpdateStmt(_) => "update",
                    _ => "other",
                })
        } else {
            None
        };

        match inner_type {
            Some("select") => self.rewrite_explain_select(input),
            Some("insert") => self.rewrite_explain_insert(input),
            Some("update") => self.rewrite_explain_update(input),
            _ => Ok(()),
        }
    }
}

impl ExplainUniqueIdRewrite {
    fn rewrite_explain_select(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        // Check if the inner SELECT needs rewriting
        let needs_rewrite = if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            if let Some(NodeEnum::SelectStmt(select)) =
                stmt.query.as_ref().and_then(|q| q.node.as_ref())
            {
                SelectUniqueIdRewrite::needs_rewrite(select)
            } else {
                false
            }
        } else {
            false
        };

        if !needs_rewrite {
            return Ok(());
        }

        let extended = input.extended();
        let mut parameter_counter = max_param_number(input.parse_result());

        if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            if let Some(NodeEnum::SelectStmt(select)) =
                stmt.query.as_mut().and_then(|q| q.node.as_mut())
            {
                input.plan().unique_ids = SelectUniqueIdRewrite::rewrite_select(
                    select,
                    extended,
                    &mut parameter_counter,
                )?;
            }
        }

        Ok(())
    }

    fn rewrite_explain_insert(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        // Check if the inner INSERT needs rewriting
        let needs_rewrite = if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            if let Some(NodeEnum::InsertStmt(insert)) =
                stmt.query.as_ref().and_then(|q| q.node.as_ref())
            {
                InsertUniqueIdRewrite::needs_rewrite(insert)
            } else {
                false
            }
        } else {
            false
        };

        if !needs_rewrite {
            return Ok(());
        }

        let extended = input.extended();
        let mut param_counter = max_param_number(input.parse_result());

        if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            if let Some(NodeEnum::InsertStmt(insert)) =
                stmt.query.as_mut().and_then(|q| q.node.as_mut())
            {
                input.plan().unique_ids =
                    InsertUniqueIdRewrite::rewrite_insert(insert, extended, &mut param_counter)?;
            }
        }

        Ok(())
    }

    fn rewrite_explain_update(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        // Check if the inner UPDATE needs rewriting
        let needs_rewrite = if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            if let Some(NodeEnum::UpdateStmt(update)) =
                stmt.query.as_ref().and_then(|q| q.node.as_ref())
            {
                UpdateUniqueIdRewrite::needs_rewrite(update)
            } else {
                false
            }
        } else {
            false
        };

        if !needs_rewrite {
            return Ok(());
        }

        let extended = input.extended();
        let mut param_counter = super::max_param_number(input.parse_result());

        if let Some(NodeEnum::ExplainStmt(stmt)) = input
            .stmt_mut()?
            .stmt
            .as_mut()
            .and_then(|stmt| stmt.node.as_mut())
        {
            if let Some(NodeEnum::UpdateStmt(update)) =
                stmt.query.as_mut().and_then(|q| q.node.as_mut())
            {
                input.plan().unique_ids =
                    UpdateUniqueIdRewrite::rewrite_update(update, extended, &mut param_counter)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::env::set_var;

    #[test]
    fn test_explain_select_unique_id() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"EXPLAIN SELECT pgdog.unique_id() AS id"#)
            .unwrap()
            .protobuf;
        let mut rewrite = ExplainUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(!query.contains("pgdog.unique_id"));
        assert!(query.contains("EXPLAIN"));
        assert!(query.contains("bigint"));
    }

    #[test]
    fn test_explain_insert_unique_id() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"EXPLAIN INSERT INTO test (id) VALUES (pgdog.unique_id())"#)
            .unwrap()
            .protobuf;
        let mut rewrite = ExplainUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(!query.contains("pgdog.unique_id"));
        assert!(query.contains("EXPLAIN"));
        assert!(query.contains("bigint"));
    }

    #[test]
    fn test_explain_update_unique_id() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt =
            pg_query::parse(r#"EXPLAIN UPDATE test SET id = pgdog.unique_id() WHERE old_id = 1"#)
                .unwrap()
                .protobuf;
        let mut rewrite = ExplainUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(!query.contains("pgdog.unique_id"));
        assert!(query.contains("EXPLAIN"));
        assert!(query.contains("bigint"));
    }

    #[test]
    fn test_explain_analyze_select_unique_id() {
        unsafe {
            set_var("NODE_ID", "pgdog-prod-1");
        }
        let stmt = pg_query::parse(r#"EXPLAIN ANALYZE SELECT pgdog.unique_id() AS id"#)
            .unwrap()
            .protobuf;
        let mut rewrite = ExplainUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(!query.contains("pgdog.unique_id"));
        assert!(query.contains("EXPLAIN"));
        assert!(query.contains("ANALYZE"));
    }

    #[test]
    fn test_explain_no_unique_id() {
        let stmt = pg_query::parse(r#"EXPLAIN SELECT 1"#).unwrap().protobuf;
        let mut rewrite = ExplainUniqueIdRewrite::default();
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        assert!(matches!(output, super::super::super::StepOutput::NoOp));
    }
}
