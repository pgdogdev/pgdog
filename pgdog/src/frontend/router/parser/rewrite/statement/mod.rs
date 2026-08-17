//! Statement rewriter.

use pg_raw_parse::{Node, NodeMut, make, nodes, transform, walk};

use crate::backend::ShardingSchema;
use crate::backend::schema::Schema;
use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::AstContext;
use crate::net::parameter::ParameterValue;

pub mod aggregate;
pub mod auto_id;
pub mod error;
pub mod insert;
pub mod offset;
pub mod plan;
pub mod simple_prepared;
pub mod unique_id;
pub mod update;

pub use error::Error;
pub use insert::InsertSplit;
pub use plan::RewritePlan;
pub(crate) use simple_prepared::PrepareExecute;
pub(crate) use update::*;

/// Statement rewrite engine context.
#[derive(Debug)]
pub struct StatementRewriteContext<'a> {
    /// The statement is using the extended protocol with placeholders.
    pub extended: bool,
    /// The statement is named, so we need to save any derivatives into the global
    /// statement cache.
    pub prepared: bool,
    /// Reference to global prepared stmt cache.
    pub prepared_statements: &'a mut PreparedStatements,
    /// Sharding schema.
    pub schema: &'a ShardingSchema,
    /// Database schema with table/column info.
    pub db_schema: &'a Schema,
    /// User name for search_path resolution.
    pub user: &'a str,
    /// Search path for table lookups.
    pub search_path: Option<&'a ParameterValue>,
}

#[derive(Debug)]
pub struct StatementRewrite<'a> {
    /// The statement was rewritten.
    rewritten: bool,
    /// Statement is using the extended protocol, so
    /// we need to rewrite function calls with parameters
    /// and not actual values.
    extended: bool,
    /// The statement is named (prepared), so we need to save
    /// any derivatives into the global statement cache.
    prepared: bool,
    /// Prepared statements cache for name mapping.
    prepared_statements: &'a mut PreparedStatements,
    /// Sharding schema for cache lookups.
    schema: &'a ShardingSchema,
    /// Database schema with table/column info.
    db_schema: &'a Schema,
    /// User name for search_path resolution.
    user: &'a str,
    /// Search path for table lookups.
    search_path: Option<&'a ParameterValue>,
}

impl<'a> StatementRewrite<'a> {
    /// Create new statement rewriter.
    ///
    /// More often than not, it won't do anything.
    ///
    pub fn new(ctx: StatementRewriteContext<'a>) -> Self {
        Self {
            rewritten: false,
            extended: ctx.extended,
            prepared: ctx.prepared,
            prepared_statements: ctx.prepared_statements,
            schema: ctx.schema,
            db_schema: ctx.db_schema,
            user: ctx.user,
            search_path: ctx.search_path,
        }
    }

    /// Create an AstContext from this rewriter's fields.
    fn ast_context(&self) -> AstContext<'a> {
        AstContext {
            sharding_schema: self.schema.clone(),
            db_schema: self.db_schema.clone(),
            user: self.user,
            search_path: self.search_path,
        }
    }

    /// Maybe rewrite the statement and produce a rewrite plan
    /// we can apply to Bind messages.
    pub fn maybe_rewrite<'mem>(
        &mut self,
        mut stmt: nodes::RawStmtMut<'mem, '_>,
        mem: make::MemoryToken<'mem>,
    ) -> Result<RewritePlan, Error> {
        let mut plan = RewritePlan::default();

        let node = stmt.stmt();
        let parameterized_stmt = match node {
            Node::InsertStmt(_)
            | Node::SelectStmt(_)
            | Node::UpdateStmt(_)
            | Node::DeleteStmt(_) => Some(node),
            Node::PrepareStmt(prepare) => {
                // Will use parameters for replacing args, not materialize values.
                self.extended = true;
                Some(prepare.query())
            }
            Node::ExecuteStmt(_) | Node::ExplainStmt(_) => None,
            // We can't do anything with DDL statements
            _ => return Ok(plan),
        };

        if let Some(parameterized_stmt) = parameterized_stmt {
            walk::walk(parameterized_stmt, |node| {
                if let Node::ParamRef(param) = node {
                    plan.params = plan.params.max(param.number as u16)
                }
            });
        }

        // Inject pgdog.unique_id() for missing BIGINT primary keys.
        // This must run BEFORE the unique_id rewriter so the injected
        // function calls get processed.
        match stmt.stmt_mut() {
            NodeMut::InsertStmt(insert) => self.inject_auto_id(insert, mem, &mut plan)?,
            NodeMut::PrepareStmt(mut prepare) => {
                if let NodeMut::InsertStmt(insert) = prepare.query_mut() {
                    self.inject_auto_id(insert, mem, &mut plan)?;
                }
            }
            _ => {}
        }

        // Track the next parameter number to use
        let mut next_param = plan.params as i32 + 1;
        let mut err = None;
        transform::transform_node(
            stmt.stmt_mut(),
            &mut transform::TransformClosure::new(|node| {
                match Self::rewrite_unique_id(node.as_ref(), mem, self.extended, &mut next_param) {
                    Ok(Some(replacement)) => {
                        plan.unique_ids += 1;
                        self.rewritten = true;
                        node.replace(replacement);
                        None
                    }
                    Err(e) => {
                        err = Some(e);
                        None
                    }
                    Ok(None) => Some(node),
                }
            }),
        );
        if let Some(err) = err {
            return Err(err);
        }

        if let NodeMut::SelectStmt(mut select) = stmt.stmt_mut() {
            self.rewrite_aggregates(&mut select, mem, &mut plan, self.db_schema)?;
            self.limit_offset(&select, &mut plan);
        }

        // Handle top-level PREPARE/EXECUTE statements.
        let prepared_result = self.rewrite_simple_prepared(stmt.stmt_mut(), mem, &mut plan)?;
        if prepared_result.rewritten {
            self.rewritten = true;
            plan.prepare_rewrites = prepared_result.rewrites;
        }

        if self.rewritten {
            plan.stmt = Some(pg_raw_parse::deparse(&*stmt)?.as_str().to_owned());
        }

        if let Node::InsertStmt(insert) = stmt.stmt() {
            self.split_insert(insert, &mut plan)?;
        }

        if let Node::UpdateStmt(stmt) = stmt.stmt() {
            self.sharding_key_update(stmt, &mut plan)?;
        }

        Ok(plan)
    }
}
