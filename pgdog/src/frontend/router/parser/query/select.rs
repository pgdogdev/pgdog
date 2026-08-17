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
        // Write overwrite because of conservative read/write split.
        let mut writes = self.write_override;
        walk::walk(stmt.into(), |node| match node {
            Node::CommonTableExpr(expr) => match expr.ctequery() {
                Node::SelectStmt(_) => (),
                _ => writes = true,
            },
            Node::LockingClause(_) => writes = true,
            Node::FuncCall(f) => {
                if let Some(f) =
                    Function::from_strings(f.funcname().into_iter().filter_map(Node::as_str))
                {
                    cross_shard = cross_shard || f.behavior().cross_shard;
                    writes = writes || f.behavior().writes;
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

        let writes = writes || !advisory_locks.is_empty();

        // Early return for any direct-to-shard queries.
        if context.shards_calculator.shard().is_direct() {
            return Ok(Command::Query(
                Route::read(context.shards_calculator.shard().clone())
                    .with_read(!writes)
                    .with_omnisharded(omnisharded)
                    .with_advisory_locks(advisory_locks),
            ));
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
            let shard = Shard::Direct(round_robin::next(context.shards));

            if let Some(recorder) = self.recorder_mut() {
                recorder.record_entry(Some(shard.clone()), "SELECT omnishard no table".to_string());
            }

            context
                .shards_calculator
                .push(ShardWithPriority::new_rr_no_table(shard));

            return Ok(Command::Query(
                Route::read(context.shards_calculator.shard().clone())
                    .with_read(!writes)
                    .with_omnisharded(omnisharded)
                    .with_advisory_locks(advisory_locks),
            ));
        }

        let order_by = Self::select_sort(stmt, context.router_context.bind);
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

                // Omnisharded by default.
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

                // Routed to a single shard via the omnisharded-by-default path
                // (non-sharded tables, including system catalogs).
                omnisharded = true;

                if let Some(recorder) = self.recorder_mut() {
                    recorder
                        .record_entry(Some(shard.clone()), format!("SELECT omnishard {}", explain));
                }

                context
                    .shards_calculator
                    .push(ShardWithPriority::new_rr_omni(shard));
            }
        }

        let mut query = Route::select(
            context.shards_calculator.shard().clone(),
            order_by,
            aggregates,
            limit,
            distinct,
        );

        // Only rewrite if query is cross-shard.
        if query.is_cross_shard() && context.shards > 1 {
            query.set_rewrite_plan(cached_ast.rewrite_plan.aggregates.clone());
        }

        Ok(Command::Query(
            query
                .with_read(!writes)
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
    ) -> Vec<OrderBy> {
        stmt.sort_clause()
            .into_iter()
            .filter_map(|sort_by| {
                use pg_raw_parse::{
                    ConstValue,
                    raw::{A_Expr_Kind::*, SortByDir::*},
                };

                let asc = matches!(sort_by.sortby_dir, SORTBY_DEFAULT | SORTBY_ASC);
                match sort_by.node() {
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
                }
            })
            .collect()
    }
}
