//! Statement rewriter.

use pg_query::protobuf::ParseResult;
use pg_query::Node;
use pgdog_config::QueryParserEngine;

use crate::backend::schema::Schema;
use crate::backend::ShardingSchema;
use crate::frontend::router::parser::AstContext;
use crate::frontend::PreparedStatements;
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
pub mod visitor;

pub use error::Error;
pub use insert::InsertSplit;
pub(crate) use plan::RewritePlan;
pub use simple_prepared::SimplePreparedResult;
pub(crate) use update::*;

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
    /// Database schema with table/column info.
    pub db_schema: &'a Schema,
    /// User name for search_path resolution.
    pub user: &'a str,
    /// Search path for table lookups.
    pub search_path: Option<&'a ParameterValue>,
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
            stmt: ctx.stmt,
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

        // Inject pgdog.unique_id() for missing BIGINT primary keys.
        // This must run BEFORE the unique_id rewriter so the injected
        // function calls get processed.
        self.inject_auto_id(&mut plan)?;

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

        self.rewrite_aggregates(&mut plan)?;
        self.limit_offset(&mut plan)?;

        if self.rewritten {
            plan.stmt = Some(match self.schema.query_parser_engine {
                QueryParserEngine::PgQueryProtobuf => self.stmt.deparse(),
                QueryParserEngine::PgQueryRaw => self.stmt.deparse_raw(),
            }?);
        }

        self.split_insert(&mut plan)?;
        self.sharding_key_update(&mut plan)?;

        Ok(plan)
    }
}
