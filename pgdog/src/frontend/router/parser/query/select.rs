use crate::frontend::router::parser::{FromClause, TablesSource, cache::Ast};

use super::*;
#[cfg(not(feature = "new_parser"))]
use function::FunctionBehavior;
#[cfg(not(feature = "new_parser"))]
use pg_query::Node as PgNode;
#[cfg(feature = "new_parser")]
use pg_raw_parse::walk;
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, nodes};
use pgdog_config::system_catalogs;
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
        stmt_old: &SelectStmt,
        #[cfg(feature = "new_parser")] stmt: &nodes::SelectStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        #[cfg(not(feature = "new_parser"))]
        let cte_writes = Self::cte_writes(stmt_old);
        #[cfg(not(feature = "new_parser"))]
        let has_locking = Self::has_locking_clause(stmt_old);
        #[cfg(not(feature = "new_parser"))]
        let FunctionBehavior {
            writes,
            cross_shard,
        } = Self::functions(stmt_old);

        #[cfg(feature = "new_parser")]
        let mut cross_shard = false;
        #[cfg(feature = "new_parser")]
        let mut writes = self.write_override;
        #[cfg(feature = "new_parser")]
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

        // Write overwrite because of conservative read/write split.
        #[cfg(not(feature = "new_parser"))]
        let writes = writes || self.write_override || cte_writes || has_locking;

        if cross_shard {
            context
                .shards_calculator
                .push(ShardWithPriority::new_override_cross_shard_function());
        }

        let (advisory_locks, mut omnisharded) = {
            let mut parser = StatementParser::from_select(
                #[cfg(not(feature = "new_parser"))]
                stmt_old,
                #[cfg(feature = "new_parser")]
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

        let (shard, is_sharded, tables) = {
            let mut statement_parser = StatementParser::from_select(
                #[cfg(not(feature = "new_parser"))]
                stmt_old,
                #[cfg(feature = "new_parser")]
                stmt.into(),
                context.router_context.bind,
                &context.sharding_schema,
                self.recorder_mut(),
            );

            let shard = statement_parser.shard()?;

            if shard.is_some() {
                (shard, true, vec![])
            } else {
                (
                    None,
                    statement_parser.is_sharded(
                        &context.router_context.schema,
                        context.router_context.cluster.user(),
                        context.router_context.parameter_hints.search_path,
                    ),
                    statement_parser.extract_tables(),
                )
            }
        };

        if let Some(shard) = shard {
            shards.insert(shard);
        }

        // SELECT NOW(), SELECT 1
        if shards.is_empty() && stmt_old.from_clause.is_empty() {
            let shard = Shard::Direct(round_robin::next() % context.shards);

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

        #[cfg(feature = "new_parser")]
        let order_by = Self::select_sort(stmt, context.router_context.bind);
        #[cfg(not(feature = "new_parser"))]
        let order_by = Self::select_sort(&stmt_old.sort_clause, context.router_context.bind);
        let from_clause = TablesSource::from(FromClause::new(&stmt_old.from_clause));

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

        #[cfg(not(feature = "new_parser"))]
        let stmt = stmt_old;
        let shard = Self::converge(&shards, ConvergeAlgorithm::default());
        let aggregates = Aggregate::parse(stmt, &context.router_context.schema);
        let limit = LimitClause::new(stmt, context.router_context.bind).limit_offset()?;
        let distinct = Distinct::new(stmt_old).distinct()?;

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
                    (context.router_context.sticky.omni_index, "sticky")
                } else {
                    (round_robin::next(), "round robin")
                };

                let shard = Shard::Direct(rr_index % context.shards);

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
    /// * `nodes`: List of pg_query-generated nodes from the ORDER BY clause.
    /// * `params`: Bind parameters, if any.
    ///
    #[cfg(feature = "new_parser")]
    fn select_sort(stmt: &nodes::SelectStmt, params: Option<&Bind>) -> Vec<OrderBy> {
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
                        let col_name = c.fields().into_iter().last()?.as_str()?;
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

    #[cfg(not(feature = "new_parser"))]
    fn select_sort(nodes: &[PgNode], params: Option<&Bind>) -> Vec<OrderBy> {
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
                        if expr.kind() == AExprKind::AexprOp
                            && let Some(node) = expr.name.first()
                            && let Some(NodeEnum::String(String { sval })) = &node.node
                        {
                            match sval.as_str() {
                                "<->" => {
                                    let mut vector: Option<Vector> = None;
                                    let mut column: Option<std::string::String> = None;

                                    for e in [&expr.lexpr, &expr.rexpr].iter().copied().flatten() {
                                        if let Ok(vec) = Value::try_from(&e.node) {
                                            match vec {
                                                Value::Placeholder(p) => {
                                                    if let Some(bind) = params
                                                        && let Ok(Some(param)) =
                                                            bind.parameter((p - 1) as usize)
                                                    {
                                                        vector = param.vector();
                                                    }
                                                }
                                                Value::Vector(vec) => vector = Some(vec),
                                                _ => (),
                                            }
                                        }

                                        if let Ok(col) = Column::try_from(&e.node) {
                                            column = Some(col.name.to_owned());
                                        }
                                    }

                                    if let Some(vector) = vector
                                        && let Some(column) = column
                                    {
                                        order_by.push(OrderBy::AscVectorL2Column(column, vector));
                                    }
                                }
                                _ => continue,
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
    #[cfg(not(feature = "new_parser"))]
    fn functions(stmt: &SelectStmt) -> FunctionBehavior {
        for target in &stmt.target_list {
            if let Ok(func) = Function::try_from(target) {
                return func.behavior();
            }
        }

        // Recurse into CTEs so a write-only function
        // nested inside a WITH clause still routes to the primary.
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref expr)) = cte.node
                    && let Some(ref query) = expr.ctequery
                    && let Some(NodeEnum::SelectStmt(ref inner)) = query.node
                {
                    let behavior = Self::functions(inner);
                    if behavior.writes {
                        return behavior;
                    }
                }
            }
        }

        FunctionBehavior::default()
    }

    /// Recursively check for a locking clause (FOR UPDATE, FOR SHARE, etc.)
    /// on this statement or any CTE nested within it.
    #[cfg(not(feature = "new_parser"))]
    fn has_locking_clause(stmt: &SelectStmt) -> bool {
        if !stmt.locking_clause.is_empty() {
            return true;
        }

        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref expr)) = cte.node
                    && let Some(ref query) = expr.ctequery
                    && let Some(NodeEnum::SelectStmt(ref inner)) = query.node
                    && Self::has_locking_clause(inner)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Check for CTEs that could trigger this query to go to a primary.
    ///
    /// # Arguments
    ///
    /// * `stmt`: SELECT statement from pg_query.
    ///
    #[cfg(not(feature = "new_parser"))]
    fn cte_writes(stmt: &SelectStmt) -> bool {
        if let Some(ref with_clause) = stmt.with_clause {
            for cte in &with_clause.ctes {
                if let Some(NodeEnum::CommonTableExpr(ref expr)) = cte.node
                    && let Some(ref query) = expr.ctequery
                    && let Some(ref node) = query.node
                {
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

        false
    }
}
