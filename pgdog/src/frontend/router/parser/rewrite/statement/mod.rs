//! Statement rewriter.

use crate::backend::schema::Schema;
use crate::config::config;
use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::rewrite::statement::plan::GeneratedParam;
use crate::net::parameter::ParameterValue;
use crate::{backend::ShardingSchema, frontend::client::QueryTimestamps};
use pg_raw_parse::{Node, NodeMut, make, nodes, transform, walk};

pub(crate) mod aggregate;
pub(crate) mod auto_id;
pub(crate) mod error;
pub(crate) mod insert;
pub(crate) mod nextval;
pub(crate) mod non_deterministic_funcs;
pub(crate) mod offset;
pub(crate) mod order_by;
pub(crate) mod plan;
pub(crate) mod projection;
pub(crate) mod simple_prepared;
pub(crate) mod unique_id;
pub(crate) mod update;

pub(crate) use error::Error;
pub(crate) use insert::InsertSplit;
use pgdog_config::RewriteMode;
//use pgdog_config::RewriteMode;
use plan::GeneratedId;
pub(crate) use plan::RewritePlan;
pub(crate) use simple_prepared::PrepareExecute;
pub(crate) use update::*;

/// Statement rewrite engine context.
#[derive(Debug)]
pub(crate) struct StatementRewriteContext<'a> {
    /// The statement is using the extended protocol with placeholders.
    pub(crate) extended: bool,
    /// The statement is named, so we need to save any derivatives into the global
    /// statement cache.
    pub(crate) prepared: bool,
    /// Reference to global prepared stmt cache.
    pub(crate) prepared_statements: &'a mut PreparedStatements,
    /// Sharding schema.
    pub(crate) schema: &'a ShardingSchema,
    /// Database schema with table/column info.
    pub(crate) db_schema: &'a Schema,
    /// User name for search_path resolution.
    pub(crate) user: &'a str,
    /// Search path for table lookups.
    pub(crate) search_path: Option<&'a ParameterValue>,
    /// Timezone for now() time generation for TIMEZONE columns.
    pub(crate) timezone: Option<&'a ParameterValue>,
    /// Statement, and transaction DateTime<Utc> relevant to the current Query (if not being cached)
    pub(crate) query_timestamps: QueryTimestamps,
}

#[derive(Debug)]
pub(crate) struct StatementRewrite<'a> {
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
    /// Timezone for now() time generation for TIMEZONE columns.
    timezone: Option<&'a ParameterValue>,
    /// Statement, and transaction DateTime<Utc> relevant to the current Query (if not being cached)
    query_timestamps: QueryTimestamps,
}

impl<'a> StatementRewrite<'a> {
    /// Create new statement rewriter.
    ///
    /// More often than not, it won't do anything.
    ///
    pub(crate) fn new(ctx: StatementRewriteContext<'a>) -> Self {
        Self {
            rewritten: false,
            extended: ctx.extended,
            prepared: ctx.prepared,
            prepared_statements: ctx.prepared_statements,
            schema: ctx.schema,
            db_schema: ctx.db_schema,
            user: ctx.user,
            search_path: ctx.search_path,
            timezone: ctx.timezone,
            query_timestamps: ctx.query_timestamps,
        }
    }

    /// Maybe rewrite the statement and produce a rewrite plan
    /// we can apply to Bind messages.
    pub(crate) fn maybe_rewrite<'mem>(
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
            NodeMut::InsertStmt(insert) => {
                self.inject_auto_id(insert, mem, &mut plan)?;
            }
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
            &mut transform::TransformClosure::new(|node| match node.as_ref() {
                Node::FuncCall(func) if Self::is_unique_id(func) => {
                    match Self::unique_id_value(mem, self.extended, &mut next_param) {
                        Ok(replacement) => {
                            plan.unique_ids += 1;
                            if self.extended {
                                plan.generated_params.push(GeneratedParam {
                                    param_num: (next_param - 1) as u16,
                                    generated_id: GeneratedId::UniqueId,
                                });
                            }
                            self.rewritten = true;
                            node.replace(replacement);
                        }
                        Err(e) => {
                            err = Some(e);
                        }
                    }
                    None
                }
                node_ref
                    if let Some(replacement) =
                        self.rewrite_sequence(node_ref, mem, &mut next_param, &mut plan) =>
                {
                    node.replace(replacement);
                    None
                }
                _ => Some(node),
            }),
        );
        if let Some(err) = err {
            return Err(err);
        }

        if let NodeMut::SelectStmt(select) = stmt.stmt_mut() {
            self.limit_offset(&select, &mut plan);
        }

        let nd_function_rewrite = !matches!(
            config().config.rewrite.non_deterministic_functions,
            RewriteMode::Ignore
        );

        if nd_function_rewrite {
            match stmt.stmt_mut() {
                NodeMut::InsertStmt(_) => {
                    self.rewrite_nd_functions(stmt.stmt_mut(), mem, &mut next_param, &mut plan)?;
                }
                NodeMut::PrepareStmt(mut prepare) => {
                    if matches!(prepare.query_mut(), NodeMut::InsertStmt(_)) {
                        self.rewrite_nd_functions(
                            prepare.query_mut(),
                            mem,
                            &mut next_param,
                            &mut plan,
                        )?;
                    }
                }
                _ => {}
            }
        }

        // Handle top-level PREPARE/EXECUTE statements.
        let prepared_result =
            self.rewrite_simple_prepared(stmt.stmt_mut(), mem, &mut plan, nd_function_rewrite)?;
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
