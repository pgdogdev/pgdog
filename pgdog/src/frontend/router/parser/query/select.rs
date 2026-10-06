use super::*;
use crate::frontend::router::parser::statement::AdvisoryLockId;
use pg_raw_parse::walk;
use pg_raw_parse::{Node, nodes};
use pgdog_config::system_catalogs;
use shared::ConvergeAlgorithm;

impl QueryParser {
    /// Handle SELECT statement.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement.
    /// * `context`: Query parser context.
    ///
    pub(super) fn select(
        &mut self,
        stmt: &nodes::SelectStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let mut cross_shard = false;
        // Does the statement itself change data: data-modifying CTEs,
        // locking clauses, write functions. This decides omnisharded
        // coverage; the primary/replica decision (`writes`) additionally
        // honours the conservative read/write split's transaction override.
        let mut mutates = false;
        walk::walk(stmt.into(), |node| match node {
            Node::CommonTableExpr(expr) => match expr.ctequery() {
                Node::SelectStmt(_) => (),
                _ => mutates = true,
            },
            Node::LockingClause(_) => mutates = true,
            Node::FuncCall(f) => {
                if let Some(f) =
                    Function::from_strings(f.funcname().into_iter().filter_map(Node::as_str))
                {
                    cross_shard = cross_shard || f.behavior().cross_shard;
                    mutates = mutates || f.behavior().writes;
                }
            }
            _ => (),
        });

        if cross_shard {
            context
                .shards_calculator
                .push(ShardWithPriority::new_override_cross_shard_function());
        }

        let mut parser = StatementParser::new(
            stmt.into(),
            context.router_context.bind,
            &context.sharding_schema,
        )
        .with_explain(self.recorder_mut().is_some());
        let (advisory_locks, mut omnisharded) =
            (parser.extract_advisory_locks(), parser.is_all_omnisharded());

        // If there's an advisory lock function in this query, we must always route it
        // to a deterministic `Shard`.
        //
        // If, specifically, it's advisory_lock_unlock_all(), broadcast to all shards.
        if !advisory_locks.is_empty() && context.shards > 1 {
            let unlock_all = advisory_locks
                .iter()
                .exactly_one()
                .map(|adv_lock| adv_lock.unlock_all)
                .unwrap_or(false);

            let shard_override = if unlock_all {
                // pg_advisory_unlock_all
                ShardWithPriority::new_override_cross_shard_function()
            } else {
                // Very simplistic direct-to-shard hashing: abs(lock_id) % shard_count
                //
                // Since advisory locks are stored in memory, we don't have to worry
                // about accounting for resharding / changing shard_count later in time
                let hashed_shard: usize = match advisory_locks
                    .iter()
                    .map(|lock| {
                        lock.id
                            .map(AdvisoryLockId::get_first_parameter)
                            .unwrap_or(0)
                            .unsigned_abs() as usize
                            % context.shards
                    })
                    .all_equal_value()
                {
                    Ok(singular_shard) => {
                        // Either we got a singular lock, or all locks hashed to the same shard.
                        singular_shard
                    }
                    Err(_) => {
                        // 2+ different shards. In this case, we error to the client,
                        // as otherwise, we'd need to do something like INSERT split, where we
                        // break apart the Client's statement, route to separate shards,
                        // and put together a response for them (+ consider deadlocks, etc).
                        // Kinda complex!
                        //
                        // Since this is not common in production, I opted to defer this
                        // in favor of quickly correcting the functionality for most common cases.
                        // TODO: eventually support something that works better
                        return Err(Error::CrossShardAdvisoryLockAttempt);
                    }
                };

                ShardWithPriority::new_override_advisory_lock(Shard::Direct(hashed_shard))
            };

            // Overrides Comment / Set / etc, since ShardSource::Override(..) sorts above them.
            context.shards_calculator.push(shard_override);
        }

        mutates |= !advisory_locks.is_empty();
        // Write override because of conservative read/write split.
        let writes = self.write_override || mutates;

        // Early return for any direct-to-shard queries.
        if context.shards_calculator.shard().is_direct() {
            return Ok(Command::Query(
                Route::read(context.shards_calculator.shard().clone())
                    .with_read(!writes)
                    .with_mutates(mutates)
                    .with_omnisharded(omnisharded)
                    .with_advisory_locks(advisory_locks),
            ));
        }

        let mut shards = HashSet::new();

        parser.set_resolved_lookups(&context.router_context.resolved_lookups);

        let shard = parser.shard()?;
        let pending_lookups = parser.take_pending_lookups();

        // Collect explain entries from the parser.
        if let Some(recorder) = self.recorder_mut() {
            recorder.extend(parser.take_explain());
        }

        let is_sharded = if shard.is_some() {
            true
        } else {
            parser.is_sharded(
                &context.router_context.schema,
                context.router_context.cluster.user(),
                context.router_context.parameter_hints.search_path,
            )
        };
        let tables = shard.is_none().then(|| parser.tables());

        context.pending_lookups.extend(pending_lookups);

        if let Some(shard) = shard {
            shards.insert(shard);
        }

        // SELECT NOW(), SELECT 1
        if shards.is_empty() && stmt.from_clause().is_empty() {
            if omnisharded && mutates {
                // e.g. `WITH ins AS (INSERT INTO omni ... RETURNING id) SELECT 1`.
                // The write must reach every shard to keep them identical.
                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(None, "SELECT omnishard write broadcasted");
                }

                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table_omni(Shard::All));
            } else {
                let shard = Shard::Direct(round_robin::next(context.shards));

                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(Some(shard.clone()), "SELECT omnishard no table");
                }

                context
                    .shards_calculator
                    .push(ShardWithPriority::new_rr_no_table(shard));
            }

            return Ok(Command::Query(
                Route::read(context.shards_calculator.shard().clone())
                    .with_read(!writes)
                    .with_mutates(mutates)
                    .with_omnisharded(omnisharded)
                    .with_advisory_locks(advisory_locks),
            ));
        }

        let from_clause_table_name = stmt.from_clause().first().and_then(|node| match node {
            Node::RangeVar(r) => Some(r.relname().expect("RangeVar always has relname")),
            _ => None,
        });

        // Shard by vector in ORDER BY clause.
        for order in stmt
            .sort_clause()
            .iter()
            .filter_map(|sort| OrderBy::parse_vector(sort.node(), context.router_context.bind))
        {
            if let Some((vector, column_name)) = order.vector() {
                for table in context.sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none() || table.name.as_deref() == from_clause_table_name)
                    {
                        let centroids = Centroids::from(&table.centroids);
                        let shard: Shard = centroids
                            .shard(vector, context.shards, table.centroid_probes)
                            .into();
                        if let Some(recorder) = self.recorder_mut() {
                            recorder.record_entry(
                                Some(shard.clone()),
                                format!("ORDER BY vector distance on {}", column_name),
                            );
                        }
                        shards.insert(shard);
                    }
                }
            }
        }

        let shard = Self::converge(&shards, ConvergeAlgorithm::default());
        let aggregates = Aggregate::parse(stmt, &context.router_context.schema);
        let limit = LimitClause::new(stmt, context.router_context.bind).limit_offset()?;
        let distinct = Distinct::new(stmt).distinct();

        if let Some(shard) = shard {
            debug!("direct-to-shard {}", shard);

            context
                .shards_calculator
                .push(ShardWithPriority::new_table(shard));
        } else if is_sharded {
            debug!("table is sharded, but no sharding key detected");

            context
                .shards_calculator
                .push(ShardWithPriority::new_table(Shard::All));
        } else {
            let system_catalog_sharded =
                context.sharding_schema.tables().is_system_catalog_sharded()
                    && tables.is_some_and(|tables| {
                        tables
                            .iter()
                            .any(|table| system_catalogs().contains(&table.name))
                    });

            if system_catalog_sharded {
                debug!("system catalog sharded");

                context
                    .shards_calculator
                    .push(ShardWithPriority::new_table(Shard::All));
            } else {
                debug!(
                    "table is not sharded, defaulting to omnisharded (schema loaded: {})",
                    context.router_context.schema.is_loaded()
                );

                // Omnisharded by default (non-sharded tables, including
                // system catalogs).
                omnisharded = true;

                if mutates {
                    // A write through an omnisharded table, e.g. a
                    // data-modifying CTE, must reach every shard to keep
                    // them identical, like an omnisharded INSERT or UPDATE.
                    if let Some(recorder) = self.recorder_mut() {
                        recorder.record_entry(None, "SELECT omnishard write broadcasted");
                    }

                    context
                        .shards_calculator
                        .push(ShardWithPriority::new_table_omni(Shard::All));
                } else {
                    // Any single shard can answer a read.
                    let sticky = tables.is_some_and(|tables| {
                        tables.iter().any(|table| {
                            context
                                .sharding_schema
                                .tables()
                                .is_omnisharded_sticky(table.name)
                                == Some(true)
                        })
                    });

                    let (rr_index, explain) = if sticky
                        || context
                            .sharding_schema
                            .tables()
                            .is_omnisharded_sticky_default()
                    {
                        (
                            context.router_context.sticky.omni_index % context.shards,
                            "sticky",
                        )
                    } else {
                        (round_robin::next(context.shards), "round robin")
                    };

                    let shard = Shard::Direct(rr_index);

                    if let Some(recorder) = self.recorder_mut() {
                        recorder.record_entry(
                            Some(shard.clone()),
                            format!("SELECT omnishard {}", explain),
                        );
                    }

                    context
                        .shards_calculator
                        .push(ShardWithPriority::new_rr_omni(shard));
                }
            }
        }

        let query = Route::select(
            context.shards_calculator.shard().clone(),
            aggregates,
            limit,
            distinct,
        );

        Ok(Command::Query(
            query
                .with_read(!writes)
                .with_mutates(mutates)
                .with_omnisharded(omnisharded)
                .with_advisory_locks(advisory_locks),
        ))
    }
}
