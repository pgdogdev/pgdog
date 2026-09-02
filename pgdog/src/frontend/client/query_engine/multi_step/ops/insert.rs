use crate::frontend::BufferedQuery;
use crate::frontend::client::query_engine::multi_step::error::{Error, InsertError};
use crate::frontend::client::query_engine::multi_step::types::{
    ForwardToClient, QueryPlanner, ResponseHistory, Step, StepRequest,
};
use crate::frontend::router::parser::rewrite::statement::Error as RewriteError;
use crate::frontend::router::parser::{AstContext, Cache};
use crate::net::{CommandComplete, Message, Parse, Query, ReadyForQuery};
use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::{
            Route,
            parser::route::{Shard, ShardWithPriority},
        },
    },
    net::Protocol,
};
use indexmap::IndexSet;
use itertools::Itertools;
use pg_raw_parse::{Node, NodeMut, deparse, make, nodes, walk};

impl QueryPlanner {
    /// Try to create a `QueryPlanner` where we suspect a multi-INSERT.
    pub(crate) fn plan_multi_insert(
        engine: &QueryEngine,
        context: &mut QueryEngineContext,
        request: &ClientRequest,
    ) -> Result<Option<QueryPlanner>, Error> {
        let Some(ref ast) = request.ast else {
            debug_assert!(false, "planner dispatched without an AST");
            return Err(InsertError::PlanMismatch.into());
        };

        let Ok(Node::InsertStmt(insert)) = ast.ast.stmts().exactly_one() else {
            debug_assert!(false, "insert_split flagged on a non-INSERT statement");
            return Err(InsertError::PlanMismatch.into());
        };

        let steps = create_steps(insert, engine, context, request)?;
        if steps.is_empty() {
            debug_assert!(false, "insert_split flagged with fewer than two tuples");
            return Err(InsertError::PlanMismatch.into());
        }
        if !Self::checks(engine, context, &steps)? {
            return Ok(None);
        }

        #[derive(Debug, Clone)]
        struct MultiInsertAggregate;

        impl ForwardToClient for MultiInsertAggregate {
            fn forward_to_client(
                &self,
                context: &QueryEngineContext,
                map: ResponseHistory,
            ) -> Vec<Message> {
                let mut messages = map.compose_all(context.client_request);

                // Sum the per-tuple INSERT tags into one.
                let rows: usize = map
                    .steps()
                    .iter()
                    .filter_map(|step| step.command_complete.as_ref())
                    .filter_map(|cc| cc.rows().ok().flatten())
                    .sum();
                messages.push(CommandComplete::new(format!("INSERT 0 {}", rows)).message());
                messages.push(ReadyForQuery::in_transaction(context.in_transaction()).message());

                messages
            }
        }

        Ok(Some(QueryPlanner {
            steps,
            forward_to_client: Some(Box::new(MultiInsertAggregate {})),
        }))
    }

    fn checks(
        engine: &QueryEngine,
        context: &mut QueryEngineContext<'_>,
        steps: &[Step],
    ) -> Result<bool, Error> {
        // All tuples map to the same shard: send the original multi-row INSERT
        // as a single statement, skipping the multi-step path entirely.
        if let Some(shard_n) = Self::uniform_shard(context.client_request, steps) {
            context.client_request.route = Some(Route::write(ShardWithPriority::new_table(
                Shard::Direct(shard_n),
            )));

            return Ok(false);
        }

        // TODO: I think this is an approximation of the old execute-time check as this is planning-based;
        //       are we connected yet? Is this suitably tested by an integration test?
        if engine.backend.connected() && !engine.backend.is_multishard() {
            return Err(InsertError::MultiShardRequired.into());
        }

        Ok(true)
    }

    /// If every split routes to the same `Shard::Direct(n)`, return that shard
    /// number. Returns `None` when the splits span multiple shards or contain
    /// any non-direct routing.
    fn uniform_shard(original_request: &ClientRequest, steps: &[Step]) -> Option<usize> {
        let direct_shard = |step: &Step| {
            let route = match &step.request {
                StepRequest::Raw => original_request.route(),
                StepRequest::Statement(statement) => &statement.route,
            };

            match route.shard() {
                Shard::Direct(n) => Some(*n),
                _ => None,
            }
        };

        let first = direct_shard(steps.first()?)?;

        steps
            .iter()
            .skip(1)
            .all(|step| direct_shard(step) == Some(first))
            .then_some(first)
    }
}

/// Split up multi-tuple INSERT statements into separate single-tuple statements
/// for individual execution.
///
/// # Example
///
/// ```sql
/// INSERT INTO my_table (id, value) VALUES ($1, $2), ($3, $4)
/// ```
///
/// becomes
///
/// ```sql
/// INSERT INTO my_table (id, value) VALUES ($1, $2)
/// INSERT INTO my_table (id, value) VALUES ($1, $2) -- These are copied from params $3 and $4
/// ```
///
pub(crate) fn create_steps(
    insert: &nodes::InsertStmt,
    engine: &QueryEngine,
    context: &mut QueryEngineContext<'_>,
    original_request: &ClientRequest,
) -> Result<Vec<Step>, Error> {
    let mut steps: Vec<Step> = Vec::new();

    let mut splits = Vec::new();
    make::try_owned(|mem| {
        let mut copy = mem.make_unique(insert);

        if let Node::SelectStmt(select) = insert.select_stmt() {
            for list in select.values_lists() {
                let (params, select) = build_single_tuple_select(mem, list);
                copy.as_mut().set_select_stmt(select.uncast());
                splits.push((params, deparse(&*copy)?.as_str().to_string()));
            }
        }

        Ok::<_, RewriteError>(copy)
    })?;

    // There's no point of continuing if there's not >= 2 splits.
    if splits.len() <= 1 {
        return Ok(steps);
    }

    // Now create Ast for each split (needs mutable borrow of prepared_statements)
    let (extended, prepared) = context
        .client_request
        .query()?
        .map(|query| (query.extended(), query.prepared()))
        .unwrap_or_default();
    let cache = Cache::get();
    let ctx = AstContext::from_cluster(engine.backend.cluster()?, context.params);

    for (params, stmt) in splits.iter() {
        let query = if extended {
            BufferedQuery::Prepared(Parse::named("", stmt))
        } else {
            BufferedQuery::Query(Query::new(stmt))
        };
        let ast = cache
            .query(&query, &ctx, &mut *context.prepared_statements)
            .map_err(|e| InsertError::Cache(e.to_string()))?;

        // If this is a named prepared statement, register the split in the global cache
        // and store the assigned name for use in Bind messages.
        let statement_name = if prepared {
            // Name will be assigned by `insert`.
            let mut parse = Parse::named("", stmt);
            context.prepared_statements.insert(&mut parse);
            Some(parse.name().to_owned())
        } else {
            None
        };

        let request = QueryPlanner::build_request(
            engine,
            context,
            original_request,
            &ast,
            stmt,
            params,
            statement_name.as_deref(),
        )?;

        steps.push(Step {
            save_key: None,
            request,
        });
    }

    Ok(steps)
}

/// Build a single-tuple INSERT from the original statement with just one values_list.
/// Returns the parameter positions (0-indexed) and the SQL string.
fn build_single_tuple_select<'mem>(
    mem: make::MemoryToken<'mem>,
    values_list: Node<'_>,
) -> (IndexSet<u16>, make::Unique<'mem, &'mem nodes::SelectStmt>) {
    let mut tuple = mem.make_unique(values_list);

    let mut params = IndexSet::new();
    walk::walk_mut(tuple.as_mut(), |node| {
        if let NodeMut::ParamRef(param) = node {
            params.insert(param.number as _);
            param.set_number(params.get_index_of(&(param.number as u16)).unwrap() as i32 + 1)
        }
    });

    let mut select = mem.make_node::<nodes::SelectStmt>();
    select.as_mut().set_values_lists(mem.make_list(&[tuple]));
    (params, select)
}
