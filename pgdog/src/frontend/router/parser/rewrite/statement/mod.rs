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
pub mod simple_to_prepared;
pub mod unique_id;
pub mod update;

pub use error::Error;
pub use insert::InsertSplit;
pub(crate) use plan::RewritePlan;
pub use simple_prepared::SimplePreparedResult;
pub(crate) use simple_to_prepared::*;
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
    /// Whether the query contains more than one SQL statement.
    pub multiple_statements: bool,
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
    /// Whether the query contains more than one SQL statement.
    multiple_statements: bool,
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
            multiple_statements: ctx.multiple_statements,
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

        // N.B. The simple to prepared rewriter should run first.
        // All subsequent rewriters will act on the prepared statement.
        self.rewrite_simple_to_prepared(stmt.stmt_mut(), mem, &mut plan)?;

        match stmt.stmt() {
            Node::InsertStmt(_)
            | Node::SelectStmt(_)
            | Node::UpdateStmt(_)
            | Node::DeleteStmt(_) => walk::walk(stmt.stmt(), |node| {
                if let Node::ParamRef(param) = node {
                    plan.num_params = plan.num_params.max(param.number as u16)
                }
            }),
            Node::PrepareStmt(_) | Node::ExecuteStmt(_) | Node::ExplainStmt(_) => {}
            // We can't do anything with DDL statements
            _ => return Ok(plan),
        }

        // Handle top-level PREPARE/EXECUTE statements.
        let prepared_result = self.rewrite_simple_prepared(stmt.stmt_mut(), mem)?;
        if prepared_result.rewritten {
            self.rewritten = true;
            plan.prepares = prepared_result.prepares;
        }

        // Inject pgdog.unique_id() for missing BIGINT primary keys.
        // This must run BEFORE the unique_id rewriter so the injected
        // function calls get processed.
        if let NodeMut::InsertStmt(insert) = stmt.stmt_mut() {
            self.inject_auto_id(insert, mem, &mut plan)?;
        }

        // Track the next parameter number to use
        let mut next_param = plan.num_params as i32 + 1;
        let mut err = None;
        transform::transform_node(
            stmt.stmt_mut(),
            &mut transform::TransformClosure::new(|node| {
                match Self::rewrite_unique_id(node.as_ref(), mem, self.extended, &mut next_param) {
                    Ok(Some(replacement)) => {
                        plan.num_unique_ids += 1;
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

        if self.rewritten && self.multiple_statements {
            return Err(Error::MultiStatementRewrite);
        }

        if self.rewritten {
            let stmt = pg_raw_parse::deparse(&*stmt)?.as_str().to_owned();

            // N.B. careful with ordering. This should run before insert splits, etc.
            // since we want to make sure the statement is registered with the global cache.
            plan.simple_to_prepared
                .step_two(self.prepared_statements, &stmt)?;

            plan.rewritten_stmt = Some(stmt);
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
