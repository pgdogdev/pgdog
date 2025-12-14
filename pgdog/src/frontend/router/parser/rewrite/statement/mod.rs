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
    pub fn new(
        stmt: &'a mut ParseResult,
        extended: bool,
        prepared_statements: &'a mut PreparedStatements,
        schema: &'a ShardingSchema,
    ) -> Self {
        Self {
            stmt,
            rewritten: false,
            extended,
            prepared_statements,
            schema,
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
        if self.prepared_statements.level.full() {
            let prepared_result = self.rewrite_simple_prepared()?;
            if prepared_result.rewritten {
                self.rewritten = true;
                plan.prepares = prepared_result.prepares;
            }
        }

        // Track the next parameter number to use
        let mut next_param = plan.params as i32 + 1;

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

        // Add helper aggregates when needed.
        self.rewrite_aggregates(&mut plan)?;

        if self.rewritten {
            plan.stmt = Some(self.stmt.deparse()?);
        }

        self.split_insert(&mut plan)?;

        Ok(plan)
    }
}
