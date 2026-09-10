use crate::frontend::router::parser::cache::Ast;

use super::*;
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
        cached_ast: &Ast,
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

        let (advisory_locks, mut omnisharded) = {
            let mut parser = StatementParser::new(
                stmt.into(),
                context.router_context.bind,
                &context.sharding_schema,
                None,
            );

            (parser.extract_advisory_locks(), parser.is_all_omnisharded())
        };

        mutates |= !advisory_locks.is_empty();
        // Write override because of conservative read/write split.
        let writes = self.write_override || mutates;

        // Early return for any direct-to-shard queries.
        if context.shards_calculator.shard().is_direct() {
<<<<<<< HEAD
            let mut route = Route::read(context.shards_calculator.shard().clone())
                .with_read(!writes)
                .with_omnisharded(omnisharded)
                .with_advisory_locks(advisory_locks);
            route.set_projection_rewrite_plan(cached_ast.rewrite_plan.projection.clone());
            return Ok(Command::Query(route));
=======
            return Ok(Command::Query(
                Route::read(context.shards_calculator.shard().clone())
                    .with_read(!writes)
                    .with_mutates(mutates)
                    .with_omnisharded(omnisharded)
                    .with_advisory_locks(advisory_locks),
            ));
>>>>>>> forkie/main
        }

        let mut shards = HashSet::new();

        let (shard, is_sharded, tables, pending_lookups) = {
            let mut statement_parser = StatementParser::new(
                stmt.into(),
                context.router_context.bind,
                &context.sharding_schema,
                self.recorder_mut(),
            );
            statement_parser.set_resolved_lookups(&context.router_context.resolved_lookups);

            let shard = statement_parser.shard()?;
            let pending_lookups = statement_parser.take_pending_lookups();

            if shard.is_some() {
                (shard, true, vec![], pending_lookups)
            } else {
                (
                    None,
                    statement_parser.is_sharded(
                        &context.router_context.schema,
                        context.router_context.cluster.user(),
                        context.router_context.parameter_hints.search_path,
                    ),
                    statement_parser.extract_tables(),
                    pending_lookups,
                )
            }
        };

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

            let mut route = Route::read(context.shards_calculator.shard().clone())
                .with_read(!writes)
                .with_mutates(mutates)
                .with_omnisharded(omnisharded)
                .with_advisory_locks(advisory_locks);
            route.set_projection_rewrite_plan(cached_ast.rewrite_plan.projection.clone());
            return Ok(Command::Query(route));
        }

        let mut order_by = Self::select_sort(stmt, context.router_context.bind);
        for helper in cached_ast.rewrite_plan.projection.order_by_helpers() {
            let Some((_, column)) = order_by
                .iter_mut()
                .find(|(position, _)| *position == helper.sort_position)
            else {
                continue;
            };
            *column = if column.asc() {
                OrderBy::Asc(helper.projected_column + 1)
            } else {
                OrderBy::Desc(helper.projected_column + 1)
            };
        }
        let order_by = order_by
            .into_iter()
            .map(|(_, order_by)| order_by)
            .collect::<Vec<_>>();
        let from_clause_table_name = stmt.from_clause().first().and_then(|node| match node {
            Node::RangeVar(r) => Some(r.relname().expect("RangeVar always has relname")),
            _ => None,
        });

        // Shard by vector in ORDER BY clause.
        for order in &order_by {
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
                if context.sharding_schema.tables().is_system_catalog_sharded() {
                    {
                        tables
                            .iter()
                            .any(|table| system_catalogs().contains(&table.name))
                    }
                } else {
                    Default::default()
                };

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
                    let sticky = tables.iter().any(|table| {
                        context
                            .sharding_schema
                            .tables()
                            .is_omnisharded_sticky(table.name)
                            == Some(true)
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

        let mut query = Route::select(
            context.shards_calculator.shard().clone(),
            order_by,
            aggregates,
            limit,
            distinct,
        );

        query.set_projection_rewrite_plan(cached_ast.rewrite_plan.projection.clone());

        Ok(Command::Query(
            query
                .with_read(!writes)
                .with_mutates(mutates)
                .with_omnisharded(omnisharded)
                .with_advisory_locks(advisory_locks),
        ))
    }

    /// Handle the `ORDER BY` clause of a `SELECT` statement.
    ///
    /// # Arguments
    ///
    /// * `nodes`: List of parser-generated nodes from the ORDER BY clause.
    /// * `params`: Statement parameters, if any.
    ///
    fn select_sort(
        stmt: &nodes::SelectStmt,
        params: Option<StatementParameters<'_>>,
    ) -> Vec<(usize, OrderBy)> {
        stmt.sort_clause()
            .into_iter()
            .enumerate()
            .filter_map(|(position, sort_by)| {
                use pg_raw_parse::{
                    ConstValue,
                    raw::{A_Expr_Kind::*, SortByDir::*},
                };

                let asc = matches!(sort_by.sortby_dir, SORTBY_DEFAULT | SORTBY_ASC);
                let order_by = match sort_by.node() {
                    Node::A_Const(c) if let Some(ConstValue::Integer(i)) = c.val() => {
                        if asc {
                            Some(OrderBy::Asc(i as _))
                        } else {
                            Some(OrderBy::Desc(i as _))
                        }
                    }

                    Node::ColumnRef(c) => {
                        // TODO: save the entire column and disambiguate
                        // when reading data with RowDescription as context.
                        let col_name = c.fields().into_iter().next_back()?.as_str()?;
                        if asc {
                            Some(OrderBy::AscColumn(col_name.into()))
                        } else {
                            Some(OrderBy::DescColumn(col_name.into()))
                        }
                    }

                    Node::A_Expr(e @ nodes::A_Expr { kind: AEXPR_OP, .. })
                        if let Some("<->") = e.name().iter().next().and_then(Node::as_str) =>
                    {
                        let mut vector: Option<Vector> = None;
                        let mut column: Option<&str> = None;

                        for e in [e.lexpr(), e.rexpr()] {
                            if let Ok(vec) = Value::try_from(e) {
                                match vec {
                                    Value::Placeholder(p) => {
                                        if let Ok(param) = params?.parameter((p - 1) as _) {
                                            vector = param?.vector();
                                        }
                                    }
                                    Value::Vector(vec) => vector = Some(vec),
                                    _ => (),
                                }
                            } else if let Ok(col) = Column::try_from(e) {
                                column = Some(col.name);
                            }
                        }

                        if let Some(vector) = vector
                            && let Some(column) = column
                        {
                            Some(OrderBy::AscVectorL2Column(column.into(), vector))
                        } else {
                            None
                        }
                    }

                    _ => None,
                };
                order_by.map(|order_by| (position, order_by))
            })
            .collect()
    }
}
