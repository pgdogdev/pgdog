//! Statement rewriter.
//!

use pg_query::protobuf::ParseResult;
use pg_query::Node;

use crate::backend::ShardingSchema;
use crate::frontend::PreparedStatements;

pub mod aggregate;
pub mod error;
pub mod insert;
pub mod plan;
pub mod simple_prepared;
pub mod unique_id;
pub mod visitor;

pub use error::Error;
pub use insert::InsertSplit;
pub use plan::RewritePlan;
pub use simple_prepared::SimplePreparedResult;

/// Statement rewrite engine context.
#[derive(Debug)]
pub struct StatementRewriteContext<'a> {
    /// The AST of the statement we are rewriting.
    pub stmt: &'a mut ParseResult,
    /// The statement is using the extended protocol with placeholders.
    pub extended: bool,
    /// The statement is named, so we need to save any derivatives into the global
    /// statement cache.
    pub prepared: bool,
    /// Reference to global prepared stmt cache.
    pub prepared_statements: &'a mut PreparedStatements,
    /// Sharding schema.
    pub schema: &'a ShardingSchema,
}

#[derive(Debug)]
pub struct StatementRewrite<'a> {
    /// SQL statement.
    stmt: &'a mut ParseResult,
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
}

impl<'a> StatementRewrite<'a> {
    /// Create new statement rewriter.
    ///
    /// More often than not, it won't do anything.
    ///
    pub fn new(ctx: StatementRewriteContext<'a>) -> Self {
        Self {
            stmt: ctx.stmt,
            rewritten: false,
            extended: ctx.extended,
            prepared: ctx.prepared,
            prepared_statements: ctx.prepared_statements,
            schema: ctx.schema,
        }
    }

    /// Maybe rewrite the statement and produce a rewrite plan
    /// we can apply to Bind messages.
    pub fn maybe_rewrite(&mut self) -> Result<RewritePlan, Error> {
        let params = visitor::count_params(self.stmt);
        let mut plan = RewritePlan {
            params,
            ..Default::default()
        };

        // Handle top-level PREPARE/EXECUTE statements.

        let prepared_result = self.rewrite_simple_prepared()?;
        if prepared_result.rewritten {
            self.rewritten = true;
            plan.prepares = prepared_result.prepares;
        }

        // Track the next parameter number to use
        let mut next_param = plan.params as i32 + 1;

        // if self.schema.rewrite.enabled {
        let extended = self.extended;
        visitor::visit_and_mutate_nodes(self.stmt, |node| -> Result<Option<Node>, Error> {
            match Self::rewrite_unique_id(node, extended, &mut next_param)? {
                Some(replacement) => {
                    plan.unique_ids += 1;
                    self.rewritten = true;
                    Ok(Some(replacement))
                }
                None => Ok(None),
            }
        })?;
        // }

        // if self.schema.rewrite.enabled {
        self.rewrite_aggregates(&mut plan)?;
        // }

        if self.rewritten {
            plan.stmt = Some(self.stmt.deparse()?);
        }

        self.split_insert(&mut plan)?;

        Ok(plan)
    }
}
