use std::{collections::HashMap, ops::Deref, sync::Arc};

use pg_query::{
    protobuf::{
        AExpr, AExprKind, AStar, ColumnRef, DeleteStmt, InsertStmt, LimitOption, List,
        OverridingKind, ParamRef, ParseResult, RangeVar, RawStmt, ResTarget, SelectStmt,
        SetOperation, String as PgString, UpdateStmt,
    },
    Node, NodeEnum,
};
use pgdog_config::RewriteMode;

use crate::{
    frontend::{
        router::{
            parser::{rewrite::statement::visitor::visit_and_mutate_nodes, Column, Table, Value},
            Ast,
        },
        BufferedQuery, ClientRequest,
    },
    net::{
        bind::Parameter, Bind, DataRow, Execute, Flush, Format, FromDataType, Parse,
        ProtocolMessage, Query, RowDescription, Sync,
    },
};

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub(crate) ast: Ast,
    pub(crate) stmt: String,
    pub(crate) params: Vec<u16>,
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

        let mut request = ClientRequest::new();

        match query {
            BufferedQuery::Query(_) => {
                request.push(Query::new(self.stmt.clone()).into());
            }
            BufferedQuery::Prepared(_) => {
                request.push(Parse::new_anonymous(&self.stmt).into());
                if let Some(params) = params {
                    request.push(self.rewrite_bind(&params)?.into());
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

#[derive(Debug, Clone)]
pub(crate) struct Inner {
    /// Fetch the whole old row.
    pub(crate) select: Statement,
    /// Check that the row actually moves shards.
    pub(crate) check: Statement,
    /// Delete old row from shard.
    pub(crate) delete: Statement,
    /// Partial insert statement.
    pub(crate) insert: Insert,
}

/// Partially built INSERT statement.
#[derive(Debug, Clone)]
pub(crate) struct Insert {
    pub(crate) table: Option<RangeVar>,
    /// Mapping of column name to `column name = value` from
    /// the original UPDATE statement.
    pub(crate) mapping: HashMap<String, Node>,
}

impl Insert {
    /// Build an INSERT statement built from an existing
    /// UPDATE statement and a row returned by a SELECT statement.
    ///
    pub(crate) fn build_request(
        &self,
        request: &ClientRequest,
        row_description: &RowDescription,
        data_row: &DataRow,
    ) -> Result<ClientRequest, Error> {
        let params = request.parameters()?;

        let mut bind = Bind::new_statement("");
        let mut columns = vec![];
        let mut values = vec![];

        for (idx, field) in row_description.iter().enumerate() {
            if let Some(value) = self.mapping.get(&field.name) {
                let value = Value::try_from(value).expect("value");
                match value {
                    Value::Placeholder(number) => {
                        let param = params
                            .as_ref()
                            .expect("param")
                            .parameter(number as usize - 1)?
                            .ok_or(Error::MissingParameter(number as u16))?;
                        bind.push_param(param.parameter().clone(), param.format())
                    }

                    Value::Integer(int) => {
                        bind.push_param(Parameter::new(int.to_string().as_bytes()), Format::Text)
                    }

                    Value::String(s) | Value::Float(s) => {
                        bind.push_param(Parameter::new(s.as_bytes()), Format::Text)
                    }

                    Value::Boolean(b) => bind.push_param(
                        Parameter::new(if b { "t".as_bytes() } else { "f".as_bytes() }),
                        Format::Text,
                    ),

                    Value::Vector(vec) => {
                        bind.push_param(Parameter::new(&vec.encode(Format::Text)?), Format::Text)
                    }

                    Value::Null => bind.push_param(Parameter::new_null(), Format::Text),
                }
            } else {
                let value = data_row.column(idx).expect("data row to have column");
                bind.push_param(Parameter::new(&value), Format::Text);
            }

            columns.push(Node {
                node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                    name: field.name.clone(),
                    ..Default::default()
                }))),
            });

            values.push(Node {
                node: Some(NodeEnum::ParamRef(ParamRef {
                    number: idx as i32 + 1,
                    ..Default::default()
                })),
            });
        }

        let insert = InsertStmt {
            relation: self.table.clone(),
            cols: columns,
            select_stmt: Some(Box::new(Node {
                node: Some(NodeEnum::SelectStmt(Box::new(SelectStmt {
                    target_list: vec![],
                    from_clause: vec![],
                    limit_option: LimitOption::Default.try_into().unwrap(),
                    where_clause: None,
                    op: SetOperation::SetopNone.try_into().unwrap(),
                    values_lists: vec![Node {
                        node: Some(NodeEnum::List(List { items: values })),
                    }],
                    ..Default::default()
                }))),
            })),
            r#override: OverridingKind::OverridingNotSet.try_into().unwrap(),
            ..Default::default()
        };

        let insert = parse_result(NodeEnum::InsertStmt(Box::new(insert)));

        Ok(ClientRequest::from(vec![
            ProtocolMessage::from(Parse::new_anonymous(&insert.deparse()?)),
            bind.into(),
            Execute::new().into(),
            Sync.into(),
        ]))
    }
}

impl<'a> StatementRewrite<'a> {
    pub(super) fn sharding_key_update(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        if self.schema.shards == 1 || self.schema.rewrite.shard_key == RewriteMode::Ignore {
            return Ok(());
        }

        let stmt = self
            .stmt
            .stmts
            .first()
            .map(|stmt| stmt.stmt.as_ref().map(|stmt| stmt.node.as_ref()))
            .flatten()
            .flatten();

        let stmt = if let Some(NodeEnum::UpdateStmt(stmt)) = stmt {
            stmt
        } else {
            return Ok(());
        };

        if let Some(value) = self.sharding_key_update_check(stmt)? {
            plan.sharding_key_update = Some(create_stmts(stmt, value)?);
        }

        Ok(())
    }

    /// Check if the sharding key could be updated.
    fn sharding_key_update_check(
        &'a self,
        stmt: &'a UpdateStmt,
    ) -> Result<Option<&'a Box<ResTarget>>, Error> {
        let table = if let Some(table) = stmt.relation.as_ref().map(Table::from) {
            table
        } else {
            return Ok(None);
        };

        Ok(stmt
            .target_list
            .iter()
            .filter(|column| {
                if let Ok(mut column) = Column::try_from(&column.node) {
                    column.qualify(table);
                    self.schema.tables().get_table(column).is_some()
                } else {
                    false
                }
            })
            .map(|column| {
                if let Some(NodeEnum::ResTarget(res)) = &column.node {
                    // Check that it's a value assignment and not something like
                    // id = id + 1
                    let supported = res
                        .val
                        .as_ref()
                        .map(|node| Value::try_from(&node.node))
                        .transpose()
                        .is_ok();

                    if supported {
                        Ok(Some(res))
                    } else {
                        Err(Error::UnsupportedShardingKeyUpdate)
                    }
                } else {
                    Ok(None)
                }
            })
            .next()
            .transpose()?
            .flatten())
    }
}

/// Visit all ParamRef nodes in a ParseResult and renumber them sequentially.
/// Returns a sorted list of the original parameter numbers.
fn rewrite_params(parse_result: &mut ParseResult) -> Result<Vec<u16>, Error> {
    let mut params = HashMap::new();

    visit_and_mutate_nodes(parse_result, |node| -> Result<Option<Node>, Error> {
        if let Some(NodeEnum::ParamRef(ref mut param)) = node.node {
            if let Some(existing) = params.get(&param.number) {
                param.number = *existing;
            } else {
                let number = params.len() as i32 + 1;
                params.insert(param.number, number);
                param.number = number;
            }
        }

        Ok(None)
    })?;

    let mut params: Vec<(i32, i32)> = params.into_iter().collect();
    params.sort_by(|a, b| a.1.cmp(&b.1));

    Ok(params
        .into_iter()
        .map(|(original, _)| original as u16)
        .collect())
}

/// # Example
///
/// ```
/// UPDATE sharded SET id = $1, email = $2 WHERE id = $3 AND user_id = $4
/// ```
///
/// ```
/// [
///   ("id", (id, $1)),
///   ("email", (email, $2))
/// ]
/// ```
///
/// This allows us to build a partial INSERT statement.
///
fn res_targets_to_insert_res_targets(stmt: &UpdateStmt) -> HashMap<String, Node> {
    stmt.target_list
        .iter()
        .map(|target| {
            let mut name = String::new();
            let mut value: Option<Box<Node>> = None;

            if let Some(ref node) = target.node {
                if let NodeEnum::ResTarget(ref target) = node {
                    value = target.val.clone();
                    name = target.name.clone();
                }
            }

            (name, *value.unwrap()) // SAFETY: We check that all ResTargets have a value.
        })
        .collect()
}

/// Convert a ResTarget (from UPDATE SET clause) to an AExpr equality expression.
///
/// Transforms `SET column = value` into `column = value` expression
/// for use in shard routing validation.
fn res_target_to_a_expr(res_target: &ResTarget) -> AExpr {
    let column_ref = ColumnRef {
        fields: vec![Node {
            node: Some(NodeEnum::String(PgString {
                sval: res_target.name.clone(),
            })),
        }],
        location: res_target.location,
    };

    AExpr {
        kind: AExprKind::AexprOp.into(),
        name: vec![Node {
            node: Some(NodeEnum::String(PgString { sval: "=".into() })),
        }],
        lexpr: Some(Box::new(Node {
            node: Some(NodeEnum::ColumnRef(column_ref)),
        })),
        rexpr: res_target.val.clone(),
        ..Default::default()
    }
}

fn select_star() -> Vec<Node> {
    vec![Node {
        node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
            name: "".into(),
            val: Some(Box::new(Node {
                node: Some(NodeEnum::ColumnRef(ColumnRef {
                    fields: vec![Node {
                        node: Some(NodeEnum::AStar(AStar {})),
                    }],
                    ..Default::default()
                })),
            })),
            ..Default::default()
        }))),
    }]
}

fn parse_result(node: NodeEnum) -> ParseResult {
    ParseResult {
        version: 170005,
        stmts: vec![RawStmt {
            stmt: Some(Box::new(Node {
                node: Some(node),
                ..Default::default()
            })),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn create_stmts(stmt: &UpdateStmt, new_value: &ResTarget) -> Result<ShardingKeyUpdate, Error> {
    let select = SelectStmt {
        target_list: select_star(),
        from_clause: vec![Node {
            node: Some(NodeEnum::RangeVar(stmt.relation.clone().unwrap())), // SAFETY: we checked the UPDATE stmt has a table name.
        }],
        limit_option: LimitOption::Default.try_into().unwrap(),
        where_clause: stmt.where_clause.clone(),
        op: SetOperation::SetopNone.try_into().unwrap(),
        ..Default::default()
    };

    let mut select = parse_result(NodeEnum::SelectStmt(Box::new(select)));

    let params = rewrite_params(&mut select)?;
    let select = pg_query::ParseResult::new(select, "".into());

    let select = Statement {
        stmt: select.deparse()?,
        ast: Ast::from_parse_result(select),
        params,
    };

    let delete = DeleteStmt {
        relation: stmt.relation.clone(),
        where_clause: stmt.where_clause.clone(),
        ..Default::default()
    };

    let mut delete = parse_result(NodeEnum::DeleteStmt(Box::new(delete)));

    let params = rewrite_params(&mut delete)?;

    let delete = pg_query::ParseResult::new(delete, "".into());

    let delete = Statement {
        stmt: delete.deparse()?.into(),
        ast: Ast::from_parse_result(delete),
        params,
    };

    let check = SelectStmt {
        target_list: select_star(),
        from_clause: vec![Node {
            node: Some(NodeEnum::RangeVar(stmt.relation.clone().unwrap())), // SAFETY: we checked the UPDATE stmt has a table name.
        }],
        limit_option: LimitOption::Default.try_into().unwrap(),
        where_clause: Some(Box::new(Node {
            node: Some(NodeEnum::AExpr(Box::new(res_target_to_a_expr(&new_value)))),
        })),
        op: SetOperation::SetopNone.try_into().unwrap(),
        ..Default::default()
    };

    let mut check = parse_result(NodeEnum::SelectStmt(Box::new(check)));
    let params = rewrite_params(&mut check)?;
    let check = pg_query::ParseResult::new(check, "".into());

    let check = Statement {
        stmt: check.deparse()?,
        ast: Ast::from_parse_result(check),
        params,
    };

    Ok(ShardingKeyUpdate {
        inner: Arc::new(Inner {
            select,
            delete,
            check,
            insert: Insert {
                table: stmt.relation.clone(),
                mapping: res_targets_to_insert_res_targets(stmt),
            },
        }),
    })
}

#[cfg(test)]
mod test {
    use pg_query::parse;
    use pgdog_config::{Rewrite, ShardedTable};

    use crate::backend::{replication::ShardedSchemas, ShardedTables};

    use super::*;

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
            ),
            schemas: ShardedSchemas::new(vec![]),
            rewrite: Rewrite {
                enabled: true,
                shard_key: RewriteMode::Rewrite,
                ..Default::default()
            },
        }
    }

    fn run_test(query: &str) -> Result<Option<ShardingKeyUpdate>, Error> {
        let mut stmt = parse(query)?;
        let schema = default_schema();
        let mut stmts = PreparedStatements::new();

        let ctx = StatementRewriteContext {
            stmt: &mut stmt.protobuf,
            schema: &schema,
            extended: true,
            prepared: false,
            prepared_statements: &mut stmts,
        };
        let mut plan = RewritePlan::default();
        StatementRewrite::new(ctx).sharding_key_update(&mut plan)?;
        Ok(plan.sharding_key_update)
    }

    #[test]
    fn test_select_basic_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        // SELECT should have WHERE clause with param renumbered to $1
        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, vec![2]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.select.params, vec![3, 5]);
    }

    #[test]
    fn test_select_single_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_delete_basic() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.delete.stmt, "DELETE FROM sharded WHERE email = $1");
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
        assert_eq!(result.select.params, Vec::<u16>::new());
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
        assert_eq!(result.select.params, vec![2, 3, 4]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.select.params, vec![20, 30]);
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
        assert_eq!(result.select.params, vec![2]);
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
        assert_eq!(result.select.params, vec![2]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.select.params, vec![2]);
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
        assert_eq!(result.select.params, vec![2]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.select.params, vec![2, 3]);
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
        assert_eq!(result.delete.params, vec![2]);
    }

    #[test]
    fn test_sharding_key_not_changed() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE id = $1 AND email = $2")
            .unwrap()
            .unwrap();
        assert_eq!(result.check.stmt, "SELECT * FROM sharded WHERE id = $1");
        assert_eq!(result.check.params, vec![1]);
    }
}
