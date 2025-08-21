use super::*;

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
        stmt: &SelectStmt,
        context: &QueryParserContext,
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

        if matches!(self.shard, Shard::Direct(_)) {
            println!("2");
            return Ok(Command::Query(
                Route::read(self.shard.clone()).set_write(writes),
            ));
        }

        // `SELECT NOW()`, `SELECT 1`, etc.
        if stmt.from_clause.is_empty() {
            println!("1");
            return Ok(Command::Query(
                Route::read(Some(round_robin::next() % context.shards)).set_write(writes),
            ));
        }

        let order_by = Self::select_sort(&stmt.sort_clause, context.router_context.bind);
        let mut shards = HashSet::new();
        let the_table = Table::try_from(&stmt.from_clause).ok();
        if let Some(where_clause) =
            WhereClause::new(the_table.as_ref().map(|t| t.name), &stmt.where_clause)
        {
            shards = Self::where_clause(
                &context.sharding_schema,
                &where_clause,
                context.router_context.bind,
            )?;
        }

        // Shard by vector in ORDER BY clause.
        for order in &order_by {
            if let Some((vector, column_name)) = order.vector() {
                for table in context.sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none()
                            || table.name.as_deref() == the_table.as_ref().map(|t| t.name))
                    {
                        let centroids = Centroids::from(&table.centroids);
                        shards.insert(centroids.shard(
                            vector,
                            context.shards,
                            table.centroid_probes,
                        ));
                    }
                }
            }
        }

        let shard = Self::converge(shards);
        let aggregates = Aggregate::parse(stmt)?;
        let limit = LimitClause::new(stmt, context.router_context.bind).limit_offset()?;
        let distinct = Distinct::new(stmt).distinct()?;

        let mut query = Route::select(shard, order_by, aggregates, limit, distinct);

        let mut omni = false;
        if query.is_all_shards() {
            if let Some(name) = the_table.as_ref().map(|t| t.name) {
                omni = context.sharding_schema.tables.omnishards().contains(name);
            }
        }

        if omni {
            query.set_shard_mut(round_robin::next() % context.shards);
        }

        Ok(Command::Query(query.set_write(writes)))
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
                if let Some(ref node) = cte.node {
                    if let NodeEnum::CommonTableExpr(expr) = node {
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
        }

        false
    }
}
