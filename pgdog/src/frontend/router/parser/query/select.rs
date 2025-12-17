use crate::frontend::router::parser::{
    cache::Ast, from_clause::FromClause, where_clause::TablesSource,
};

use super::*;
use shared::ConvergeAlgorithm;

impl QueryParser {
    /// Handle SELECT statement.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    /// * `context`: Query parser context.
    ///
    pub(super) fn select(
        &mut self,
        cached_ast: &Ast,
        stmt: &SelectStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let cte_writes = Self::cte_writes(stmt);
        let mut writes = Self::functions(stmt)?;

        // Write overwrite because of conservative read/write split.
        if self.write_override {
            writes.writes = true;
        }

        if cte_writes {
            writes.writes = true;
        }

        // Early return for any direct-to-shard queries.
        if self.shard.shard().is_direct() {
            return Ok(Command::Query(
                Route::read(self.shard.shard().clone()).with_write(writes),
            ));
        }

        let mut shards = HashSet::new();

        let shard = StatementParser::from_select(
            stmt,
            context.router_context.bind,
            &context.sharding_schema,
            self.recorder_mut(),
        )
        .shard()?;

        if let Some(shard) = shard {
            shards.insert(shard);
        }

        // `SELECT NOW()`, `SELECT 1`, etc.
        if shards.is_empty() && stmt.from_clause.is_empty() {
            self.shard.push(ShardWithPriority::new_rr(Shard::Direct(
                round_robin::next() % context.shards,
            )));

            return Ok(Command::Query(
                Route::read(self.shard.shard().clone()).with_write(writes),
            ));
        }

        let order_by = Self::select_sort(&stmt.sort_clause, context.router_context.bind);
        let from_clause = TablesSource::from(FromClause::new(&stmt.from_clause));

        // Shard by vector in ORDER BY clause.
        for order in &order_by {
            if let Some((vector, column_name)) = order.vector() {
                for table in context.sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none()
                            || table.name.as_deref() == from_clause.table_name())
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
        let aggregates = Aggregate::parse(stmt);
        let limit = LimitClause::new(stmt, context.router_context.bind).limit_offset()?;
        let distinct = Distinct::new(stmt).distinct()?;

        self.shard.push(ShardWithPriority::new_table(shard));

        let mut query = Route::select(
            self.shard.shard().clone(),
            order_by,
            aggregates,
            limit,
            distinct,
        );

        // Omnisharded tables check.
        if query.is_all_shards() {
            let tables = from_clause.tables();
            let mut sticky = false;
            let omni = tables.iter().all(|table| {
                let is_sticky = context.sharding_schema.tables.omnishards().get(table.name);

                if let Some(is_sticky) = is_sticky {
                    if *is_sticky {
                        sticky = true;
                    }
                    true
                } else {
                    false
                }
            });

            if omni {
                let shard = if sticky {
                    context.router_context.sticky.omni_index
                } else {
                    round_robin::next()
                } % context.shards;

                self.shard
                    .push(ShardWithPriority::new_omni(Shard::Direct(shard)));

                query.set_shard_mut(self.shard.shard().clone());

                if let Some(recorder) = self.recorder_mut() {
                    recorder.record_entry(
                        Some(shard.into()),
                        format!(
                            "SELECT matched omnisharded tables: {}",
                            tables
                                .iter()
                                .map(|table| table.name)
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    );
                }
            }
        }

        // Only rewrite if query is cross-shard.
        if query.is_cross_shard() && context.shards > 1 {
            query.with_aggregate_rewrite_plan_mut(cached_ast.rewrite_plan.aggregates.clone());
        }

        Ok(Command::Query(query.with_write(writes)))
    }

    /// Handle the `ORDER BY` clause of a `SELECT` statement.
    ///
    /// # Arguments
    ///
    /// * `nodes`: List of pg_query-generated nodes from the ORDER BY clause.
    /// * `params`: Bind parameters, if any.
    ///
    fn select_sort(nodes: &[Node], params: Option<&Bind>) -> Vec<OrderBy> {
        let mut order_by = vec![];
        for clause in nodes {
            if let Some(NodeEnum::SortBy(ref sort_by)) = clause.node {
                let asc = matches!(sort_by.sortby_dir, 0..=2);
                let Some(ref node) = sort_by.node else {
                    continue;
                };
                let Some(ref node) = node.node else {
                    continue;
                };

                match node {
                    NodeEnum::AConst(aconst) => {
                        if let Some(Val::Ival(ref integer)) = aconst.val {
                            order_by.push(if asc {
                                OrderBy::Asc(integer.ival as usize)
                            } else {
                                OrderBy::Desc(integer.ival as usize)
                            });
                        }
                    }

                    NodeEnum::ColumnRef(column_ref) => {
                        // TODO: save the entire column and disambiguate
                        // when reading data with RowDescription as context.
                        let Some(field) = column_ref.fields.last() else {
                            continue;
                        };
                        if let Some(NodeEnum::String(ref string)) = field.node {
                            order_by.push(if asc {
                                OrderBy::AscColumn(string.sval.clone())
                            } else {
                                OrderBy::DescColumn(string.sval.clone())
                            });
                        }
                    }

                    NodeEnum::AExpr(expr) => {
                        if expr.kind() == AExprKind::AexprOp {
                            if let Some(node) = expr.name.first() {
                                if let Some(NodeEnum::String(String { sval })) = &node.node {
                                    match sval.as_str() {
                                        "<->" => {
                                            let mut vector: Option<Vector> = None;
                                            let mut column: Option<std::string::String> = None;

                                            for e in
                                                [&expr.lexpr, &expr.rexpr].iter().copied().flatten()
                                            {
                                                if let Ok(vec) = Value::try_from(&e.node) {
                                                    match vec {
                                                        Value::Placeholder(p) => {
                                                            if let Some(bind) = params {
                                                                if let Ok(Some(param)) =
                                                                    bind.parameter((p - 1) as usize)
                                                                {
                                                                    vector = param.vector();
                                                                }
                                                            }
                                                        }
                                                        Value::Vector(vec) => vector = Some(vec),
                                                        _ => (),
                                                    }
                                                };

                                                if let Ok(col) = Column::try_from(&e.node) {
                                                    column = Some(col.name.to_owned());
                                                }
                                            }

                                            if let Some(vector) = vector {
                                                if let Some(column) = column {
                                                    order_by.push(OrderBy::AscVectorL2Column(
                                                        column, vector,
                                                    ));
                                                }
                                            }
                                        }
                                        _ => continue,
                                    }
                                }
                            }
                        }
                    }

                    _ => continue,
                }
            }
        }

        order_by
    }

    /// Handle Postgres functions that could trigger the SELECT to go to a primary.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    ///
    fn functions(stmt: &SelectStmt) -> Result<FunctionBehavior, Error> {
        for target in &stmt.target_list {
            if let Ok(func) = Function::try_from(target) {
                return Ok(func.behavior());
            }
        }

        Ok(if stmt.locking_clause.is_empty() {
            FunctionBehavior::default()
        } else {
            FunctionBehavior::writes_only()
        })
    }

    /// Check for CTEs that could trigger this query to go to a primary.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    ///
    fn cte_writes(stmt: &SelectStmt) -> bool {
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref expr)) = cte.node {
                    if let Some(ref query) = expr.ctequery {
                        if let Some(ref node) = query.node {
                            match node {
                                NodeEnum::SelectStmt(stmt) => {
                                    if Self::cte_writes(stmt) {
                                        return true;
                                    }
                                }

                                _ => {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }

        false
    }
}
