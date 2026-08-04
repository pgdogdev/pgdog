use indexmap::IndexSet;
use std::{ops::Deref, sync::Arc};

use pg_raw_parse::make::{owned, try_owned};
use pg_raw_parse::{DeparseResult, Node, NodeMut, Owned, deparse, nodes, walk};
use pgdog_config::RewriteMode;

use crate::{
    frontend::{
        BufferedQuery, ClientRequest,
        router::{
            Ast,
            parser::{Column, Table, Value},
            sharding::ShardedTable,
        },
    },
    net::{
        Bind, DataRow, Describe, Execute, Flush, Format, Parse, ProtocolMessage, Query,
        RowDescription, Sync, bind::Parameter,
    },
};

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub(crate) ast: Ast,
    pub(crate) stmt: String,
    pub(crate) params: IndexSet<u16>,
}

impl Statement {
    /// Create new Bind message for the statement from original Bind.
    pub(crate) fn rewrite_bind(&self, bind: &Bind) -> Result<Bind, Error> {
        let mut new = Bind::new_statement(""); // We use anonymous prepared
        // statements for execution.
        for param in &self.params {
            let param = bind
                .parameter(*param as usize - 1)?
                .ok_or(Error::MissingParameter(*param))?;
            new.push_param(param.parameter().clone(), param.format());
        }

        Ok(new)
    }

    /// Build request from statement.
    ///
    /// Use the same protocol as the original statement.
    ///
    pub(crate) fn build_request(&self, request: &ClientRequest) -> Result<ClientRequest, Error> {
        let query = request.query()?.ok_or(Error::EmptyQuery)?;
        let params = request.parameters()?;

        let mut request = ClientRequest::default();

        match query {
            BufferedQuery::Query(_) => {
                request.push(Query::new(self.stmt.clone()).into());
            }
            BufferedQuery::Prepared(_) => {
                request.push(Parse::new_anonymous(&self.stmt).into());
                request.push(Describe::new_statement("").into());
                if let Some(params) = params {
                    request.push(self.rewrite_bind(params)?.into());
                    request.push(Execute::new().into());
                    request.push(Sync.into());
                } else {
                    // This shouldn't really happen since we don't rewrite
                    // non-executable requests.
                    request.push(Flush.into());
                }
            }
        }

        request.ast = Some(self.ast.clone());

        Ok(request)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShardingKeyUpdate {
    inner: Arc<Inner>,
}

impl Deref for ShardingKeyUpdate {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ShardingKeyUpdate {
    pub(crate) fn sharded_table<'a>(
        &self,
        sharded_tables: &'a [ShardedTable],
    ) -> Option<&'a ShardedTable> {
        let table = self.target_table();

        sharded_tables.iter().find(|sharded| {
            if let Some(name) = sharded.name.as_ref()
                && !table.name_match(name)
            {
                return false;
            }

            if let Some(schema) = sharded.schema.as_ref()
                && let Some(table_schema) = table.schema
                && table_schema != schema
            {
                return false;
            }

            self.from_update
                .target_list()
                .iter()
                .any(|rt| rt.name() == Some(&*sharded.column))
        })
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    /// Fetch the whole old row.
    pub(crate) select: Statement,
    /// Check that the row actually moves shards.
    pub(crate) check: Statement,
    /// Delete old row from shard.
    pub(crate) delete: Statement,
    /// Update this is being constructed from
    // FIXME(sage): There's no reason we need to own this, but this struct is
    // ultimately a child of AstInner, where the statement is borrowed from,
    // so we can't add a lifetime here. We should see if we can pass the update
    // in later as needed
    from_update: Owned<nodes::UpdateStmt>,
}

impl Inner {
    pub(crate) fn target_table(&self) -> Table<'_> {
        Table::from(
            self.from_update
                .relation()
                .expect("UPDATE always has table"),
        )
    }

    /// Build an INSERT statement built from an existing
    /// UPDATE statement and a row returned by a SELECT statement.
    pub(crate) fn build_insert_request(
        &self,
        request: &ClientRequest,
        row_description: &RowDescription,
        data_row: &DataRow,
    ) -> Result<ClientRequest, Error> {
        let params = request.parameters()?;
        let mut bind = Bind::new_statement("");

        let insert = try_owned(|mem| -> Result<_, Error> {
            let mut columns = Vec::new();
            let mut values = Vec::new();
            for (idx, field) in row_description.iter().enumerate() {
                columns.push(
                    mem.make_res_target(Some(&*field.name), mem.empty(), mem.none())
                        .uncast(),
                );

                if let Some(value) = self.from_update.target_list().iter().find_map(|rt| {
                    if rt.name() == Some(&*field.name) {
                        Some(rt.val())
                    } else {
                        None
                    }
                }) {
                    if let Ok(Value::Placeholder(number)) = Value::try_from(value) {
                        let param = params
                            .and_then(|p| p.parameter(number as usize - 1).transpose())
                            .ok_or(Error::MissingParameter(number as u16))??;
                        bind.push_param(param.parameter().clone(), param.format());
                        values.push(mem.make_param_ref(bind.params_raw().len() as _).uncast());
                    } else {
                        values.push(mem.make_unique(value));
                    }
                } else {
                    // This column wasn't changed, get the value from the select
                    let value = data_row.get_raw(idx).ok_or(Error::MissingColumn(idx))?;

                    if value.is_null() {
                        bind.push_param(Parameter::new_null(), Format::Text);
                    } else {
                        bind.push_param(Parameter::new(value), Format::Text);
                    }
                    values.push(mem.make_param_ref(bind.params_raw().len() as _).uncast());
                }
            }

            let mut insert = mem.make_node::<nodes::InsertStmt>();
            insert
                .as_mut()
                .set_relation(mem.make_unique(self.from_update.relation()));
            insert.as_mut().set_cols(mem.make_list(&columns));
            let mut select = mem.make_node::<nodes::SelectStmt>();
            select
                .as_mut()
                .set_values_lists(mem.make_list(&[mem.make_list(&values)]));
            insert.as_mut().set_select_stmt(select.uncast());
            insert
                .as_mut()
                .set_returning_clause(mem.make_unique(self.from_update.returning_clause()));
            Ok(mem.make_list(&[mem.make_raw_stmt(insert.uncast())]))
        })?;
        let stmt = deparse(insert.first().unwrap())?;

        //// Build the AST to be used with the router.
        //// It's identical to the string-generated statement above.
        let ast = Ast::from_raw_stmts(insert);

        let mut req = ClientRequest::from(vec![
            ProtocolMessage::from(Parse::new_anonymous(stmt.as_str())),
            Describe::new_statement("").into(), // So we get both T and t,
            bind.into(),
            Execute::new().into(),
            Sync.into(),
        ]);
        req.ast = Some(ast);
        Ok(req)
    }

    /// Do we have to return the rows to the client?
    pub(crate) fn is_returning(&self) -> bool {
        self.from_update.returning_clause().is_some()
    }
}

impl<'a> StatementRewrite<'a> {
    /// Create a plan for shardking key updates, if we suspect there is one
    /// in the query.
    pub(super) fn sharding_key_update(
        &mut self,
        stmt: &nodes::UpdateStmt,
        plan: &mut RewritePlan,
    ) -> Result<(), Error> {
        if self.schema.shards == 1 || self.schema.rewrite.shard_key == RewriteMode::Ignore {
            return Ok(());
        }

        if let Some(value) = self.sharding_key_update_check(stmt)? {
            // Without a WHERE clause, this is a huge
            // cross-shard rewrite.
            if let Node::None = stmt.where_clause() {
                return Err(Error::WhereClauseMissing);
            }
            plan.sharding_key_update = Some(create_stmts(stmt, value)?);
        }

        Ok(())
    }

    /// Check if the sharding key could be updated.
    fn sharding_key_update_check(
        &'a self,
        stmt: &'a nodes::UpdateStmt,
    ) -> Result<Option<&'a nodes::ResTarget>, Error> {
        let table = stmt
            .relation()
            .map(Table::from)
            .expect("UPDATE always has a table");

        let Some(shard_key_assignment) = stmt.target_list().into_iter().find(|c| {
            Column::try_from(*c).is_ok_and(|mut c| {
                c.qualify(table);
                self.schema.tables().get_table(c).is_some()
            })
        }) else {
            return Ok(None);
        };

        // Check that it's a value assignment and not something like
        // id = id + 1
        if Value::try_from(shard_key_assignment.val()).is_ok() {
            Ok(Some(shard_key_assignment))
        } else {
            let expr = shard_key_assignment.val();
            let expr = deparse_expr([expr])?;
            // FIXME:
            //
            // We can technically support this. We can inject this into
            // the `SELECT` statement we use to pull the existing row
            // and use the computed value for assignment.
            Err(Error::UnsupportedShardingKeyUpdate(format!(
                "\"{}\" = {}",
                shard_key_assignment.name().unwrap_or_default(),
                expr.as_str().strip_prefix("SELECT ").unwrap_or("<unknown>"),
            )))
        }
    }
}

/// Visit all ParamRef nodes in a ParseResult and renumber them sequentially.
/// Returns a sorted list of the original parameter numbers.
fn rewrite_params(node: NodeMut<'_, '_>) -> IndexSet<u16> {
    let mut params = IndexSet::new();
    walk::walk_mut(node, |node| {
        if let NodeMut::ParamRef(param) = node {
            params.insert(param.number as _);
            param.set_number(params.get_index_of(&(param.number as u16)).unwrap() as i32 + 1)
        }
    });
    params
}

/// Deparse an expression node by wrapping it in a SELECT statement.
fn deparse_expr<'a>(nodes: impl IntoIterator<Item = Node<'a>>) -> Result<DeparseResult, Error> {
    let node = owned(|mem| {
        let mut select = mem.make_node::<nodes::SelectStmt>();
        let res_targets = nodes
            .into_iter()
            .map(|node| match node {
                Node::ResTarget(r) => mem.make_unique(r),
                _ => mem.make_res_target(None, mem.empty(), mem.make_unique(node)),
            })
            .collect::<Vec<_>>();
        select.as_mut().set_target_list(mem.make_list(&res_targets));
        mem.make_raw_stmt(select.uncast())
    });
    deparse(&*node).map_err(Into::into)
}

fn create_stmts<'a>(
    stmt: &'a nodes::UpdateStmt,
    new_value: &'a nodes::ResTarget,
) -> Result<ShardingKeyUpdate, Error> {
    let select_star = owned(|mem| {
        let mut select_stmt = mem.make_node::<nodes::SelectStmt>();
        select_stmt.as_mut().set_target_list(
            mem.make_list(&[mem.make_res_target(
                None,
                mem.empty(),
                mem.make_column_ref(mem.make_list(&[mem.make_node::<nodes::A_Star>().uncast()]))
                    .uncast(),
            )]),
        );
        select_stmt.as_mut().set_from_clause(
            mem.make_list(&[mem
                .make_unique(stmt.relation().expect("UPDATE always has a table"))
                .uncast()]),
        );
        select_stmt
    });

    let mut params = IndexSet::new();
    let select = owned(|mem| {
        let mut select_stmt = mem.make_unique(&*select_star);
        select_stmt
            .as_mut()
            .set_where_clause(mem.make_unique(stmt.where_clause()));
        params = rewrite_params(select_stmt.as_mut().into());
        mem.make_list(&[mem.make_raw_stmt(select_stmt.uncast())])
    });

    let select = Statement {
        stmt: deparse(select.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(select),
        params,
    };

    let mut params = IndexSet::new();
    let delete = owned(|mem| {
        let mut delete = mem.make_node::<nodes::DeleteStmt>();
        delete
            .as_mut()
            .set_relation(mem.make_unique(stmt.relation()));
        delete
            .as_mut()
            .set_where_clause(mem.make_unique(stmt.where_clause()));
        params = rewrite_params(delete.as_mut().into());
        mem.make_list(&[mem.make_raw_stmt(delete.uncast())])
    });

    let delete = Statement {
        stmt: deparse(delete.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(delete),
        params,
    };

    let mut params = IndexSet::new();
    let check = owned(|mem| {
        let mut select_stmt = mem.make_unique(&*select_star);
        select_stmt.as_mut().set_where_clause(
            mem.make_a_expr(
                nodes::A_Expr_Kind::AEXPR_OP,
                mem.make_list(&[mem.make_string(Some("=")).uncast()]),
                mem.make_column_ref(mem.make_list(&[mem.make_string(new_value.name()).uncast()]))
                    .uncast(),
                mem.make_unique(new_value.val()),
            )
            .uncast(),
        );
        params = rewrite_params(select_stmt.as_mut().into());
        mem.make_list(&[mem.make_raw_stmt(select_stmt.uncast())])
    });

    let check = Statement {
        stmt: deparse(check.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(check),
        params,
    };

    Ok(ShardingKeyUpdate {
        inner: Arc::new(Inner {
            select,
            delete,
            check,
            from_update: owned(|mem| mem.make_unique(stmt)),
        }),
    })
}

#[cfg(test)]
mod test {
    use crate::frontend::router::sharding::ShardedTable;
    use indexmap::indexset;
    use pgdog_config::Rewrite;

    use crate::backend::schema::Schema;
    use crate::backend::{ShardedTables, replication::ShardedSchemas};
    use crate::net::messages::row_description::Field;

    use super::*;

    fn default_db_schema() -> Schema {
        Schema::default()
    }

    fn default_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    database: "pgdog".into(),
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
                false,
                pgdog_config::SystemCatalogsBehavior::default(),
            ),
            schemas: ShardedSchemas::new(vec![]),
            rewrite: Rewrite {
                enabled: true,
                shard_key: RewriteMode::Rewrite,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn run_test(query: &str) -> Result<Option<ShardingKeyUpdate>, Error> {
        let stmt = pg_raw_parse::parse(query)?;
        let schema = default_schema();
        let db_schema = default_db_schema();
        let mut stmts = PreparedStatements::new();

        let ctx = StatementRewriteContext {
            schema: &schema,
            db_schema: &db_schema,
            extended: true,
            prepared: false,
            prepared_statements: &mut stmts,
            user: "",
            search_path: None,
        };
        let mut plan = RewritePlan::default();
        StatementRewrite::new(ctx).sharding_key_update(
            match stmt.stmts().next().unwrap() {
                Node::UpdateStmt(stmt) => stmt,
                _ => panic!("Not an update"),
            },
            &mut plan,
        )?;
        Ok(plan.sharding_key_update)
    }

    #[test]
    fn test_select_basic_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        // SELECT should have WHERE clause with param renumbered to $1
        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, indexset![2]);

        let schema = default_schema();
        let tables = schema.tables.tables();
        assert_eq!(result.target_table().name, "sharded");
        assert_eq!(result.sharded_table(tables).unwrap().column, "id");
    }

    #[test]
    fn test_select_multiple_where_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
        assert!(!result.is_returning());
    }

    #[test]
    fn test_select_non_sequential_params() {
        // Params in WHERE are $3 and $5, should be renumbered to $1 and $2
        let result = run_test(
            "UPDATE sharded SET id = $1, value = $2, other = $4 WHERE email = $3 AND name = $5",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, indexset![3, 5]);
    }

    #[test]
    fn test_select_single_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, indexset![2]);
    }

    #[test]
    fn test_delete_basic() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.delete.stmt, "DELETE FROM sharded WHERE email = $1");

        assert!(result.sharded_table(&[]).is_none());
        assert!(
            result
                .sharded_table(&[ShardedTable {
                    name: Some("other".into()),
                    column: "id".into(),
                    ..Default::default()
                }])
                .is_none()
        );
        assert!(
            result
                .sharded_table(&[ShardedTable {
                    name: Some("sharded".into()),
                    column: "user_id".into(),
                    ..Default::default()
                }])
                .is_none()
        );
    }

    #[test]
    fn test_delete_multiple_where_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 AND name = $2"
        );
    }

    #[test]
    fn test_no_params_in_where() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = 'test@example.com'")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = 'test@example.com'"
        );
        assert!(result.select.params.is_empty());
    }

    #[test]
    fn test_where_with_in_clause() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $4)")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $3)"
        );
        assert_eq!(result.select.params, indexset![2, 3, 4]);
    }

    #[test]
    fn test_where_with_comparison_operators() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE count > $2 AND count < $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE count > $1 AND count < $2"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
    }

    #[test]
    fn test_where_with_or_condition() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $2"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
    }

    #[test]
    fn test_high_param_numbers() {
        let result = run_test("UPDATE sharded SET id = $10 WHERE email = $20 AND name = $30")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, indexset![20, 30]);
    }

    #[test]
    fn test_non_sharding_key_update_returns_none() {
        // Updating a non-sharding column should return None
        let result = run_test("UPDATE sharded SET email = $1 WHERE id = $2").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_where_with_like() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email LIKE $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email LIKE $1"
        );
        assert_eq!(result.select.params, indexset![2]);
    }

    #[test]
    fn test_where_with_is_null() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND deleted_at IS NULL")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND deleted_at IS NULL"
        );
        assert_eq!(result.select.params, indexset![2]);
    }

    #[test]
    fn test_where_with_between() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE created_at BETWEEN $2 AND $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE created_at BETWEEN $1 AND $2"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
    }

    #[test]
    fn test_same_param_used_twice() {
        // Same parameter $2 used twice in WHERE clause
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2")
            .unwrap()
            .unwrap();

        // Both occurrences should be renumbered to $1
        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $1"
        );
        // Only one unique param in the mapping
        assert_eq!(result.select.params, indexset![2]);
    }

    #[test]
    fn test_same_param_used_multiple_times() {
        // $2 used three times
        let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $2 AND c = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $1 AND c = $1"
        );
        assert_eq!(result.select.params, indexset![2]);
    }

    #[test]
    fn test_mixed_repeated_and_unique_params() {
        // $2 used twice, $3 used once
        let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $3 AND c = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $2 AND c = $1"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
    }

    #[test]
    fn test_repeated_params_in_in_clause() {
        // Same param repeated in IN clause (unusual but valid)
        let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $2)")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $1)"
        );
        assert_eq!(result.select.params, indexset![2, 3]);
    }

    #[test]
    fn test_delete_with_repeated_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 OR name = $1"
        );
        assert_eq!(result.delete.params, indexset![2]);
    }

    #[test]
    fn test_sharding_key_not_changed() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE id = $1 AND email = $2")
            .unwrap()
            .unwrap();
        assert_eq!(result.check.stmt, "SELECT * FROM sharded WHERE id = $1");
        assert_eq!(result.check.params, indexset![1]);
    }

    #[test]
    fn test_unsupported_assignment() {
        let result = run_test("UPDATE sharded SET id = random() WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = random()"
        );
    }

    #[test]
    fn test_unsupported_assignment_arithmetic_add() {
        let result = run_test("UPDATE sharded SET id = id + 1 WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = id + 1"
        );
    }

    #[test]
    fn test_unsupported_assignment_arithmetic_multiply() {
        let result = run_test("UPDATE sharded SET id = id * 2 WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = id * 2"
        );
    }

    #[test]
    fn test_unsupported_assignment_arithmetic_with_param() {
        let result = run_test("UPDATE sharded SET id = id + $2 WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = id + $2"
        );
    }

    #[test]
    fn test_unsupported_assignment_now() {
        let result = run_test("UPDATE sharded SET id = now() WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = now()"
        );
    }

    #[test]
    fn test_unsupported_assignment_coalesce() {
        let result = run_test("UPDATE sharded SET id = coalesce(id, 0) WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = COALESCE(id, 0)"
        );
    }

    #[test]
    fn test_unsupported_assignment_case() {
        let result =
            run_test("UPDATE sharded SET id = CASE WHEN id > 0 THEN 1 ELSE 0 END WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = CASE WHEN id > 0 THEN 1 ELSE 0 END"
        );
    }

    #[test]
    fn test_unsupported_assignment_subquery() {
        let result =
            run_test("UPDATE sharded SET id = (SELECT max(id) FROM sharded) WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = (SELECT max(id) FROM sharded)"
        );
    }

    #[test]
    fn test_unsupported_assignment_column_reference() {
        let result = run_test("UPDATE sharded SET id = other_column WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = other_column"
        );
    }

    #[test]
    fn test_unsupported_assignment_concat() {
        let result = run_test("UPDATE sharded SET id = id || '_suffix' WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = id || '_suffix'"
        );
    }

    #[test]
    fn test_unsupported_assignment_negation() {
        let result = run_test("UPDATE sharded SET id = -id WHERE id = $1");
        std::assert_matches!(
            result,
            Err(Error::UnsupportedShardingKeyUpdate(msg)) if msg == "\"id\" = - id"
        );
    }

    #[test]
    fn test_insert_build_request_with_expr_column() {
        // Test that INSERT statement is built correctly when there are expression columns.
        // The expression should appear directly in the VALUES clause.
        // Use literal values (not placeholders) to avoid needing bind parameters.
        let result = run_test("UPDATE sharded SET id = 42, email = random() WHERE id = 1")
            .unwrap()
            .unwrap();

        // Create a mock row description matching the SELECT * result
        let row_description = RowDescription::new(&[
            Field::bigint("id"),
            Field::text("email"),
            Field::text("other_col"),
            Field::text("other_other_col"),
        ]);

        // Create a mock data row with values for columns not in the UPDATE SET clause
        let mut data_row = DataRow::new();
        data_row.add("1"); // id - will be overwritten by mapping
        data_row.add("old@example.com"); // email - will be overwritten by mapping
        data_row.add("other_value"); // other_col - from existing row
        data_row.add("other_other_value"); // other_other_col - from existing row

        // Create a simple query request (not prepared statement)
        let request = ClientRequest::from(vec![ProtocolMessage::from(Query::new(
            "UPDATE sharded SET id = 42, email = random() WHERE id = 1",
        ))]);

        let insert_request = result
            .build_insert_request(&request, &row_description, &data_row)
            .unwrap();

        // Get the query from the request to verify the INSERT statement
        let query = insert_request.query().unwrap().unwrap();
        let stmt = query.query();

        // The INSERT should contain the expression random() directly in VALUES
        assert!(
            stmt.contains("random()"),
            "INSERT statement should contain the expression: {}",
            stmt
        );
        // Verify it's an INSERT statement
        assert!(
            stmt.starts_with("INSERT INTO"),
            "Should be an INSERT statement: {}",
            stmt
        );
        // Verify parameter numbering is correct: $1 for id, random() for email, $2 for other_col
        // (not $3, which would be wrong if we used row index instead of bind param index)
        assert!(
            stmt.contains("$1") && stmt.contains("$2") && !stmt.contains("$3"),
            "Parameter numbering should be sequential without gaps: {}",
            stmt
        );
    }
}
