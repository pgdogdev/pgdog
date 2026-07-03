#[cfg(not(feature = "new_parser"))]
use std::collections::HashMap;
use std::{ops::Deref, sync::Arc};

#[cfg(not(feature = "new_parser"))]
use pg_query::{
    Node as PgNode, NodeEnum,
    protobuf::{
        AExpr, AExprKind, AStar, ColumnRef, DeleteStmt, InsertStmt, LimitOption, List,
        OverridingKind, ParamRef, ParseResult, RangeVar, RawStmt, ResTarget, SelectStmt,
        SetOperation, String as PgString, UpdateStmt,
    },
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{
    DeparseResult, Node, Owned, deparse,
    make::owned,
    nodes,
    walk::{self, Recurse},
};
#[cfg(not(feature = "new_parser"))]
use pgdog_config::QueryParserEngine;
use pgdog_config::RewriteMode;

#[cfg(not(feature = "new_parser"))]
use crate::frontend::router::parser::rewrite::statement::visitor::visit_and_mutate_nodes;
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
        Bind, DataRow, Describe, Execute, Flush, Format, FromDataType, Parse, ProtocolMessage,
        Query, RowDescription, Sync, bind::Parameter,
    },
};

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub(crate) ast: Ast,
    pub(crate) stmt: String,
    #[cfg(not(feature = "new_parser"))]
    pub(crate) params: Vec<u16>,
}

impl Statement {
    /// Create new Bind message for the statement from original Bind.
    #[cfg(feature = "new_parser")]
    pub(crate) fn rewrite_bind(&self, bind: &Bind) -> Result<Bind, Error> {
        let mut new = bind.clone();
        new.anonymize();
        Ok(new)
    }

    /// Create new Bind message for the statement from original Bind.
    #[cfg(not(feature = "new_parser"))]
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

            #[cfg(feature = "new_parser")]
            {
                self.from_update
                    .targetList()
                    .iter()
                    .any(|rt| rt.name() == Some(&*sharded.column))
            }
            #[cfg(not(feature = "new_parser"))]
            {
                self.insert.mapping.contains_key(&sharded.column)
            }
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
    /// Partial insert statement.
    #[cfg(not(feature = "new_parser"))]
    pub(crate) insert: Insert,
    /// Update this is being constructed from
    #[cfg(feature = "new_parser")]
    // FIXME(sage): There's no reason we need to own this, but this struct is
    // ultimately a child of AstInner, where the statement is borrowed from,
    // so we can't add a lifetime here. We should see if we can pass the update
    // in later as needed
    from_update: Owned<nodes::UpdateStmt>,
}

impl Inner {
    #[cfg(feature = "new_parser")]
    pub(crate) fn target_table(&self) -> Table<'_> {
        Table::from(
            self.from_update
                .relation()
                .expect("UPDATE always has table"),
        )
    }

    #[cfg(not(feature = "new_parser"))]
    pub(crate) fn target_table(&self) -> Table<'_> {
        Table::from(&self.insert.table)
    }

    /// Build an INSERT statement built from an existing
    /// UPDATE statement and a row returned by a SELECT statement.
    #[cfg(feature = "new_parser")]
    pub(crate) fn build_insert_request(
        &self,
        request: &ClientRequest,
        row_description: &RowDescription,
        data_row: &DataRow,
    ) -> Result<ClientRequest, Error> {
        let params = request.parameters()?;

        let mut bind = Bind::new_statement("");
        let mut columns = Vec::new();
        let mut values = Vec::new();
        let mut columns_str = Vec::new();
        let mut values_str = Vec::new();

        let mut bind_idx = 1;
        for (row_idx, field) in row_description.iter().enumerate() {
            columns_str.push(format!(r#""{}""#, field.name.replace("\"", "\"\""))); // Escape "

            if let Some(value) = self.from_update.targetList().iter().find_map(|rt| {
                if rt.name() == Some(&*field.name) {
                    Some(rt.val())
                } else {
                    None
                }
            }) {
                let Ok(value) = Value::try_from(value) else {
                    values_str.push(
                        deparse_expr([value])?
                            .as_str()
                            .trim_start_matches("SELECT ")
                            .to_owned(),
                    );
                    continue;
                };

                values_str.push(format!("${bind_idx}"));
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

                    Value::String(s) => bind.push_param(Parameter::new(s.as_bytes()), Format::Text),

                    Value::Float(f) => {
                        bind.push_param(Parameter::new(f.to_string().as_bytes()), Format::Text)
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
                let value = data_row
                    .get_raw(row_idx)
                    .ok_or(Error::MissingColumn(row_idx))?;

                if value.is_null() {
                    bind.push_param(Parameter::new_null(), Format::Text);
                } else {
                    bind.push_param(Parameter::new(value), Format::Text);
                }

                values_str.push(format!("${bind_idx}"));
            }

            columns.push(&*field.name);
            values.push(bind_idx);
            bind_idx += 1;
        }

        let returning_list = self.from_update.returningList();
        let insert = owned(|mem| {
            let mut insert = mem.make_node::<nodes::InsertStmt>();
            insert
                .as_mut()
                .set_relation(mem.make_unique(self.from_update.relation()));
            let cols = columns
                .into_iter()
                .map(|c| {
                    mem.make_ResTarget(Some(c), mem.empty(), mem.none())
                        .uncast()
                })
                .collect::<Vec<_>>();
            insert.as_mut().set_cols(mem.make_List(&cols));
            let mut select = mem.make_node::<nodes::SelectStmt>();
            let values = values
                .into_iter()
                .map(|v| mem.make_ParamRef(v).uncast())
                .collect::<Vec<_>>();
            select
                .as_mut()
                .set_valuesLists(mem.make_List(&[mem.make_List(&values)]));
            insert.as_mut().set_selectStmt(select.uncast());
            insert
                .as_mut()
                .set_returningList(mem.make_unique(returning_list));
            mem.make_List(&[mem.make_RawStmt(insert.uncast())])
        });

        let stmt = format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            self.target_table(),
            columns_str.join(", "),
            values_str.join(", "),
            if returning_list.is_empty() {
                String::from("")
            } else {
                format!(
                    " RETURNING {}",
                    deparse_expr(returning_list)?
                        .as_str()
                        .trim_start_matches("SELECT ")
                )
            }
        );

        //// Build the AST to be used with the router.
        //// It's identical to the string-generated statement above.
        let ast = Ast::from_raw_stmts(insert);

        let mut req = ClientRequest::from(vec![
            ProtocolMessage::from(Parse::new_anonymous(&stmt)),
            Describe::new_statement("").into(), // So we get both T and t,
            bind.into(),
            Execute::new().into(),
            Sync.into(),
        ]);
        req.ast = Some(ast);
        Ok(req)
    }

    #[cfg(not(feature = "new_parser"))]
    /// Build an INSERT statement built from an existing
    /// UPDATE statement and a row returned by a SELECT statement.
    pub(crate) fn build_insert_request(
        &self,
        request: &ClientRequest,
        row_description: &RowDescription,
        data_row: &DataRow,
    ) -> Result<ClientRequest, Error> {
        self.insert
            .build_request(request, row_description, data_row)
    }

    #[cfg(feature = "new_parser")]
    /// Do we have to return the rows to the client?
    pub(crate) fn is_returning(&self) -> bool {
        !self.from_update.returningList().is_empty()
    }

    #[cfg(not(feature = "new_parser"))]
    /// Do we have to return the rows to the client?
    pub(crate) fn is_returning(&self) -> bool {
        !self.insert.returning_list.is_empty() && self.insert.returnin_list_deparsed.is_some()
    }
}

/// Partially built INSERT statement.
#[derive(Debug)]
#[cfg(not(feature = "new_parser"))]
pub(crate) struct Insert {
    pub(super) table: RangeVar,
    /// Mapping of column name to `column name = value` from
    /// the original UPDATE statement.
    pub(super) mapping: HashMap<String, UpdateValue>,
    /// Return columns.
    pub(super) returning_list: Vec<PgNode>,
    /// Returning list deparsed.
    pub(super) returnin_list_deparsed: Option<String>,
}

#[cfg(not(feature = "new_parser"))]
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
        let mut columns_str = vec![];
        let mut values_str = vec![];

        let mut bind_idx = 0;
        for (row_idx, field) in row_description.iter().enumerate() {
            columns_str.push(format!(r#""{}""#, field.name.replace("\"", "\"\""))); // Escape "

            if let Some(value) = self.mapping.get(&field.name) {
                let value = match value {
                    UpdateValue::Value(value) => {
                        values_str.push(format!("${}", bind_idx + 1));
                        Value::try_from(value.as_ref()).unwrap() // SAFETY: We check that the value is valid.
                    }
                    UpdateValue::Expr(expr) => {
                        values_str.push(expr.clone());
                        continue;
                    }
                };

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

                    Value::String(s) => bind.push_param(Parameter::new(s.as_bytes()), Format::Text),

                    Value::Float(f) => {
                        bind.push_param(Parameter::new(f.to_string().as_bytes()), Format::Text)
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
                let value = data_row
                    .get_raw(row_idx)
                    .ok_or(Error::MissingColumn(row_idx))?;

                if value.is_null() {
                    bind.push_param(Parameter::new_null(), Format::Text);
                } else {
                    bind.push_param(Parameter::new(value), Format::Text);
                }

                values_str.push(format!("${}", bind_idx + 1));
            }

            columns.push(PgNode {
                node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                    name: field.name.clone(),
                    ..Default::default()
                }))),
            });

            values.push(PgNode {
                node: Some(NodeEnum::ParamRef(ParamRef {
                    number: bind_idx + 1,
                    ..Default::default()
                })),
            });

            bind_idx += 1;
        }

        let insert = InsertStmt {
            relation: Some(self.table.clone()),
            cols: columns,
            select_stmt: Some(Box::new(PgNode {
                node: Some(NodeEnum::SelectStmt(Box::new(SelectStmt {
                    target_list: vec![],
                    from_clause: vec![],
                    limit_option: LimitOption::Default.into(),
                    where_clause: None,
                    op: SetOperation::SetopNone.into(),
                    values_lists: vec![PgNode {
                        node: Some(NodeEnum::List(List { items: values })),
                    }],
                    ..Default::default()
                }))),
            })),
            returning_list: self.returning_list.clone(),
            r#override: OverridingKind::OverridingNotSet.into(),
            ..Default::default()
        };

        let table = Table::from(&self.table);

        // This is probably one of the few places in the code where
        // we shouldn't use the parser. It's quicker to concatenate strings
        // than to call pg_query::deparse because of the Protobuf (de)ser.
        //
        // TODO: Replace protobuf (de)ser with native mappings and use the
        // parser again.
        //
        let stmt = format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            table,
            columns_str.join(", "),
            values_str.join(", "),
            if let Some(ref returning_list) = self.returnin_list_deparsed {
                format!("RETURNING {}", returning_list)
            } else {
                "".into()
            }
        );

        // Build the AST to be used with the router.
        // It's identical to the string-generated statement above.
        let insert = parse_result(NodeEnum::InsertStmt(Box::new(insert)));
        let insert = pg_query::ParseResult::new(insert, "".into());

        let ast = Ast::from_parse_result(insert);

        let mut req = ClientRequest::from(vec![
            ProtocolMessage::from(Parse::new_anonymous(&stmt)),
            Describe::new_statement("").into(), // So we get both T and t,
            bind.into(),
            Execute::new().into(),
            Sync.into(),
        ]);
        req.ast = Some(ast);
        Ok(req)
    }
}

impl<'a> StatementRewrite<'a> {
    /// Create a plan for shardking key updates, if we suspect there is one
    /// in the query.
    pub(super) fn sharding_key_update(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        if self.schema.shards == 1 || self.schema.rewrite.shard_key == RewriteMode::Ignore {
            return Ok(());
        }

        #[cfg(not(feature = "new_parser"))]
        let Some(NodeEnum::UpdateStmt(stmt)) = self
            .stmt
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref().map(|stmt| stmt.node.as_ref()))
            .flatten()
        else {
            // TODO: Handle EXPLAIN ANALYZE which needs to execute.
            // We could return a combined plan for all 3 queries
            // we need to execute.
            return Ok(());
        };

        #[cfg(feature = "new_parser")]
        let Some(Node::UpdateStmt(stmt)) = self.new_stmt.stmts().next() else {
            // TODO: Handle EXPLAIN ANALYZE which needs to execute.
            // We could return a combined plan for all 3 queries
            // we need to execute.
            return Ok(());
        };

        if let Some(value) = self.sharding_key_update_check(stmt)? {
            // Without a WHERE clause, this is a huge
            // cross-shard rewrite.
            #[cfg(feature = "new_parser")]
            if let Node::None = stmt.whereClause() {
                return Err(Error::WhereClauseMissing);
            }
            #[cfg(not(feature = "new_parser"))]
            if stmt.where_clause.is_none() {
                return Err(Error::WhereClauseMissing);
            }
            plan.sharding_key_update = Some(create_stmts(
                stmt,
                value,
                #[cfg(not(feature = "new_parser"))]
                self.schema.query_parser_engine,
            )?);
        }

        Ok(())
    }

    /// Check if the sharding key could be updated.
    #[cfg(feature = "new_parser")]
    fn sharding_key_update_check(
        &'a self,
        stmt: &'a nodes::UpdateStmt,
    ) -> Result<Option<&'a nodes::ResTarget>, Error> {
        let table = stmt
            .relation()
            .map(Table::from)
            .expect("UPDATE always has a table");

        let Some(shard_key_assignment) = stmt.targetList().into_iter().find(|c| {
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

    #[cfg(not(feature = "new_parser"))]
    fn sharding_key_update_check(
        &'a self,
        stmt: &'a UpdateStmt,
    ) -> Result<Option<&'a ResTarget>, Error> {
        let table = if let Some(table) = stmt.relation.as_ref().map(Table::from) {
            table
        } else {
            return Ok(None);
        };

        Ok(stmt
            .target_list
            .iter()
            .filter(|column| match Column::try_from(&column.node) {
                Ok(mut column) => {
                    column.qualify(table);
                    self.schema.tables().get_table(column).is_some()
                }
                _ => false,
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
                        Ok(Some(res.as_ref()))
                    } else {
                        // FIXME:
                        //
                        // We can technically support this. We can inject this into
                        // the `SELECT` statement we use to pull the existing row
                        // and use the computed value for assignment.
                        //
                        let expr = res
                            .val
                            .as_ref()
                            .map(|node| deparse_expr_old(node, self.schema.query_parser_engine))
                            .transpose()?
                            .unwrap_or_else(|| "<unknown>".to_string());
                        Err(Error::UnsupportedShardingKeyUpdate(format!(
                            "\"{}\" = {}",
                            res.name, expr
                        )))
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
#[cfg(not(feature = "new_parser"))]
fn rewrite_params(parse_result: &mut ParseResult) -> Result<Vec<u16>, Error> {
    let mut params = HashMap::new();

    visit_and_mutate_nodes(parse_result, |node| -> Result<Option<PgNode>, Error> {
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
    params.sort_by_key(|a| a.1);

    Ok(params
        .into_iter()
        .map(|(original, _)| original as u16)
        .collect())
}

#[derive(Debug, Clone)]
#[cfg(not(feature = "new_parser"))]
pub(super) enum UpdateValue {
    Value(Box<PgNode>),
    Expr(String), // We deparse the expression because we can't handle it yet.
}

/// # Example
///
/// ```ignore
/// UPDATE sharded SET id = $1, email = $2 WHERE id = $3 AND user_id = $4
/// ```
///
/// ```ignore
/// [
///   ("id", (id, $1)),
///   ("email", (email, $2))
/// ]
/// ```
///
/// This allows us to build a partial INSERT statement.
///
#[cfg(not(feature = "new_parser"))]
fn res_targets_to_insert_res_targets(
    stmt: &UpdateStmt,
    query_parser_engine: QueryParserEngine,
) -> Result<HashMap<String, UpdateValue>, Error> {
    let mut result = HashMap::new();
    for target in &stmt.target_list {
        if let Some(NodeEnum::ResTarget(target)) = target.node.as_ref() {
            let valid = target
                .val
                .as_ref()
                .map(|value| Value::try_from(&value.node).is_ok())
                .unwrap_or_default();
            let value = if valid {
                UpdateValue::Value(target.val.clone().unwrap())
            } else {
                UpdateValue::Expr(deparse_expr_old(
                    target.val.as_ref().unwrap(),
                    query_parser_engine,
                )?)
            };
            result.insert(target.name.clone(), value);
        }
    }

    Ok(result)
}

/// Convert a ResTarget (from UPDATE SET clause) to an AExpr equality expression.
///
/// Transforms `SET column = value` into `column = value` expression
/// for use in shard routing validation.
#[cfg(not(feature = "new_parser"))]
fn res_target_to_a_expr(res_target: &ResTarget) -> AExpr {
    let column_ref = ColumnRef {
        fields: vec![PgNode {
            node: Some(NodeEnum::String(PgString {
                sval: res_target.name.clone(),
            })),
        }],
        location: res_target.location,
    };

    AExpr {
        kind: AExprKind::AexprOp.into(),
        name: vec![PgNode {
            node: Some(NodeEnum::String(PgString { sval: "=".into() })),
        }],
        lexpr: Some(Box::new(PgNode {
            node: Some(NodeEnum::ColumnRef(column_ref)),
        })),
        rexpr: res_target.val.clone(),
        ..Default::default()
    }
}

#[cfg(not(feature = "new_parser"))]
fn select_star() -> Vec<PgNode> {
    vec![PgNode {
        node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
            name: "".into(),
            val: Some(Box::new(PgNode {
                node: Some(NodeEnum::ColumnRef(ColumnRef {
                    fields: vec![PgNode {
                        node: Some(NodeEnum::AStar(AStar {})),
                    }],
                    ..Default::default()
                })),
            })),
            ..Default::default()
        }))),
    }]
}

#[cfg(not(feature = "new_parser"))]
fn parse_result(node: NodeEnum) -> ParseResult {
    ParseResult {
        version: pg_query::PG_VERSION_NUM as i32,
        stmts: vec![RawStmt {
            stmt: Some(Box::new(PgNode { node: Some(node) })),
            stmt_location: 0,
            stmt_len: 0,
        }],
    }
}

/// Deparse an expression node by wrapping it in a SELECT statement.
#[cfg(feature = "new_parser")]
fn deparse_expr<'a>(nodes: impl IntoIterator<Item = Node<'a>>) -> Result<DeparseResult, Error> {
    let node = owned(|mem| {
        let mut select = mem.make_node::<nodes::SelectStmt>();
        let res_targets = nodes
            .into_iter()
            .map(|node| match node {
                Node::ResTarget(r) => mem.make_unique(r),
                _ => mem.make_ResTarget(None, mem.empty(), mem.make_unique(node)),
            })
            .collect::<Vec<_>>();
        select.as_mut().set_targetList(mem.make_List(&res_targets));
        mem.make_RawStmt(select.uncast())
    });
    deparse(&*node).map_err(Into::into)
}

#[cfg(not(feature = "new_parser"))]
fn deparse_expr_old(
    node: &PgNode,
    query_parser_engine: QueryParserEngine,
) -> Result<String, Error> {
    Ok(deparse_list(
        &[PgNode {
            node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                val: Some(Box::new(node.clone())),
                ..Default::default()
            }))),
        }],
        query_parser_engine,
    )?
    .unwrap()) // SAFETY: we are not passing in an empty list.
}

/// Deparse a list of expressions by wrapping them into a SELECT statement.
#[cfg(not(feature = "new_parser"))]
fn deparse_list(
    list: &[PgNode],
    query_parser_engine: QueryParserEngine,
) -> Result<Option<String>, Error> {
    if list.is_empty() {
        return Ok(None);
    }

    let stmt = SelectStmt {
        target_list: list.to_vec(),
        limit_option: LimitOption::Default.into(),
        op: SetOperation::SetopNone.into(),
        ..Default::default()
    };
    let result = parse_result(NodeEnum::SelectStmt(Box::new(stmt)));
    let string = match query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => result.deparse()?,
        QueryParserEngine::PgQueryRaw => result.deparse_raw()?,
    }
    .strip_prefix("SELECT ")
    .unwrap_or_default()
    .to_string();

    Ok(Some(string))
}

#[cfg(feature = "new_parser")]
fn create_stmts<'a>(
    stmt: &'a nodes::UpdateStmt,
    new_value: &'a nodes::ResTarget,
) -> Result<ShardingKeyUpdate, Error> {
    let has_bind_params = walk::walk_manual(stmt.into(), |node| match node {
        Node::ParamRef(_) => std::ops::ControlFlow::Break(()),
        _ => Recurse::yes(),
    })
    .is_some();

    // Stick the original UPDATE in a CTE with `AND false` at the end of the
    // WHERE clause to ensure all bind parameters are used, and have their
    // types inferred the same way they would have in the original query
    let use_all_params = if has_bind_params {
        Some(owned(|mem| {
            let mut noop_query = mem.make_unique(stmt);
            noop_query.as_mut().set_whereClause(
                mem.make_BoolExpr(
                    nodes::BoolExprType::AND_EXPR,
                    mem.make_List(&[
                        mem.make_A_Const(pg_raw_parse::ConstValue::Boolean(false))
                            .uncast(),
                        mem.make_unique(stmt.whereClause()),
                    ]),
                )
                .uncast(),
            );
            let cte = mem.make_CommonTableExpr(
                "__pgdog_use_bind_params",
                mem.empty(),
                noop_query.uncast(),
            );
            mem.make_WithClause(mem.make_List(&[cte]), false)
        }))
    } else {
        None
    };

    let select_star = owned(|mem| {
        let mut select_stmt = mem.make_node::<nodes::SelectStmt>();
        select_stmt.as_mut().set_targetList(
            mem.make_List(&[mem.make_ResTarget(
                None,
                mem.empty(),
                mem.make_ColumnRef(mem.make_List(&[mem.make_node::<nodes::A_Star>().uncast()]))
                    .uncast(),
            )]),
        );
        select_stmt.as_mut().set_fromClause(
            mem.make_List(&[mem
                .make_unique(stmt.relation().expect("UPDATE always has a table"))
                .uncast()]),
        );
        select_stmt
            .as_mut()
            .set_withClause(mem.make_unique(use_all_params.as_deref()));
        select_stmt
    });

    let select = owned(|mem| {
        let mut select_stmt = mem.make_unique(&*select_star);
        select_stmt
            .as_mut()
            .set_whereClause(mem.make_unique(stmt.whereClause()));
        // let params = rewrite_params(&mut select)?;
        mem.make_List(&[mem.make_RawStmt(select_stmt.uncast())])
    });

    let select = Statement {
        stmt: deparse(select.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(select),
    };

    let delete = owned(|mem| {
        let mut delete = mem.make_node::<nodes::DeleteStmt>();
        delete
            .as_mut()
            .set_relation(mem.make_unique(stmt.relation()));
        delete
            .as_mut()
            .set_whereClause(mem.make_unique(stmt.whereClause()));
        delete
            .as_mut()
            .set_withClause(mem.make_unique(use_all_params.as_deref()));
        // let params = rewrite_params(&mut delete)?;
        mem.make_List(&[mem.make_RawStmt(delete.uncast())])
    });

    let delete = Statement {
        stmt: deparse(delete.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(delete),
    };

    let check = owned(|mem| {
        let mut select_stmt = mem.make_unique(&*select_star);
        select_stmt.as_mut().set_whereClause(
            mem.make_A_Expr(
                nodes::A_Expr_Kind::AEXPR_OP,
                mem.make_List(&[mem.make_String(Some("=")).uncast()]),
                mem.make_ColumnRef(mem.make_List(&[mem.make_String(new_value.name()).uncast()]))
                    .uncast(),
                mem.make_unique(new_value.val()),
            )
            .uncast(),
        );
        // let params = rewrite_params(&mut select_stmt)?;
        mem.make_List(&[mem.make_RawStmt(select_stmt.uncast())])
    });

    let check = Statement {
        stmt: deparse(check.first().unwrap())?.as_str().to_owned(),
        ast: Ast::from_raw_stmts(check),
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

#[cfg(not(feature = "new_parser"))]
fn create_stmts(
    stmt: &UpdateStmt,
    new_value: &ResTarget,
    query_parser_engine: QueryParserEngine,
) -> Result<ShardingKeyUpdate, Error> {
    let select = SelectStmt {
        target_list: select_star(),
        from_clause: vec![PgNode {
            node: Some(NodeEnum::RangeVar(stmt.relation.clone().unwrap())), // SAFETY: we checked the UPDATE stmt has a table name.
        }],
        limit_option: LimitOption::Default.into(),
        where_clause: stmt.where_clause.clone(),
        op: SetOperation::SetopNone.into(),
        ..Default::default()
    };

    let mut select = parse_result(NodeEnum::SelectStmt(Box::new(select)));

    let params = rewrite_params(&mut select)?;
    let select = pg_query::ParseResult::new(select, "".into());

    let select = Statement {
        stmt: match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => select.deparse()?,
            QueryParserEngine::PgQueryRaw => select.deparse_raw()?,
        },
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
        stmt: match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => delete.deparse()?,
            QueryParserEngine::PgQueryRaw => delete.deparse_raw()?,
        },
        ast: Ast::from_parse_result(delete),
        params,
    };

    let check = SelectStmt {
        target_list: select_star(),
        from_clause: vec![PgNode {
            node: Some(NodeEnum::RangeVar(stmt.relation.clone().unwrap())), // SAFETY: we checked the UPDATE stmt has a table name.
        }],
        limit_option: LimitOption::Default.into(),
        where_clause: Some(Box::new(PgNode {
            node: Some(NodeEnum::AExpr(Box::new(res_target_to_a_expr(new_value)))),
        })),
        op: SetOperation::SetopNone.into(),
        ..Default::default()
    };

    let mut check = parse_result(NodeEnum::SelectStmt(Box::new(check)));
    let params = rewrite_params(&mut check)?;
    let check = pg_query::ParseResult::new(check, "".into());

    let check = Statement {
        stmt: match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => check.deparse()?,
            QueryParserEngine::PgQueryRaw => check.deparse_raw()?,
        },
        ast: Ast::from_parse_result(check),
        params,
    };

    Ok(ShardingKeyUpdate {
        inner: Arc::new(Inner {
            select,
            delete,
            check,
            insert: Insert {
                table: stmt.relation.clone().expect("UPDATE always has table"),
                mapping: res_targets_to_insert_res_targets(stmt, query_parser_engine)?,
                returning_list: stmt.returning_list.clone(),
                returnin_list_deparsed: deparse_list(&stmt.returning_list, query_parser_engine)?,
            },
        }),
    })
}

#[cfg(test)]
mod test {
    use crate::frontend::router::sharding::ShardedTable;
    use pg_query::parse;
    use pgdog_config::Rewrite;
    use regex::Regex;

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
        let mut stmt_old = parse(query)?;
        #[cfg(feature = "new_parser")]
        let stmt = pg_raw_parse::parse(query)?;
        let schema = default_schema();
        let db_schema = default_db_schema();
        let mut stmts = PreparedStatements::new();

        let ctx = StatementRewriteContext {
            stmt: &mut stmt_old.protobuf,
            #[cfg(feature = "new_parser")]
            new_stmt: &stmt,
            schema: &schema,
            db_schema: &db_schema,
            extended: true,
            prepared: false,
            prepared_statements: &mut stmts,
            user: "",
            search_path: None,
        };
        let mut plan = RewritePlan::default();
        StatementRewrite::new(ctx).sharding_key_update(&mut plan)?;
        Ok(plan.sharding_key_update)
    }

    #[test]
    fn test_select_basic_where_param() {
        let sql = "UPDATE sharded SET id = $1 WHERE email = $2";
        let result = run_test(sql).unwrap().unwrap();

        // SELECT should have WHERE clause with param renumbered to $1
        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(sql, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);

        let schema = default_schema();
        let tables = schema.tables.tables();
        assert_eq!(result.target_table().name, "sharded");
        assert_eq!(result.sharded_table(tables).unwrap().column, "id");
    }

    #[test]
    fn test_select_multiple_where_params() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
        assert!(!result.is_returning());
    }

    #[test]
    fn test_select_non_sequential_params() {
        // Resulting query must be valid with the client's bind params, even
        // though the select stmt will only use $3 and $5
        let query =
            "UPDATE sharded SET id = $1, value = $2, other = $4 WHERE email = $3 AND name = $5";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![3, 5]);
    }

    #[test]
    fn test_select_single_where_param() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_delete_basic() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(&result.delete.stmt, "DELETE FROM sharded WHERE email = $1");
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.delete.stmt);

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
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 AND name = $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.delete.stmt);
    }

    #[test]
    fn test_no_params_in_where() {
        let query = "UPDATE sharded SET id = $1 WHERE email = 'test@example.com'";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = 'test@example.com'",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, Vec::<u16>::new());
    }

    #[test]
    fn test_where_with_in_clause() {
        let query = "UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $4)";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $3)",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3, 4]);
    }

    #[test]
    fn test_where_with_comparison_operators() {
        let query = "UPDATE sharded SET id = $1 WHERE count > $2 AND count < $3";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE count > $1 AND count < $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_where_with_or_condition() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 OR name = $3";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_high_param_numbers() {
        let query = "UPDATE sharded SET id = $10 WHERE email = $20 AND name = $30";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
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
        let query = "UPDATE sharded SET id = $1 WHERE email LIKE $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email LIKE $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_where_with_is_null() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 AND deleted_at IS NULL";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND deleted_at IS NULL",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_where_with_between() {
        let query = "UPDATE sharded SET id = $1 WHERE created_at BETWEEN $2 AND $3";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE created_at BETWEEN $1 AND $2",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_same_param_used_twice() {
        // Same parameter $2 used twice in WHERE clause
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2";
        let result = run_test(query).unwrap().unwrap();

        // Both occurrences should be renumbered to $1
        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $1",
        );
        // Only one unique param in the mapping
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_same_param_used_multiple_times() {
        // $2 used three times
        let query = "UPDATE sharded SET id = $1 WHERE a = $2 AND b = $2 AND c = $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $1 AND c = $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_mixed_repeated_and_unique_params() {
        // $2 used twice, $3 used once
        let query = "UPDATE sharded SET id = $1 WHERE a = $2 AND b = $3 AND c = $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $2 AND c = $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_repeated_params_in_in_clause() {
        // Same param repeated in IN clause (unusual but valid)
        let query = "UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $2)";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $1)",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.select.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_delete_with_repeated_params() {
        let query = "UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2";
        let result = run_test(query).unwrap().unwrap();

        assert_equivalent_sql(
            &result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 OR name = $1",
        );
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.delete.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.delete.params, vec![2]);
    }

    #[test]
    fn test_sharding_key_not_changed() {
        let query = "UPDATE sharded SET id = $1 WHERE id = $1 AND email = $2";
        let result = run_test(query).unwrap().unwrap();
        assert_equivalent_sql(&result.check.stmt, "SELECT * FROM sharded WHERE id = $1");
        #[cfg(feature = "new_parser")]
        assert_all_bind_params_used(query, &result.check.stmt);
        #[cfg(not(feature = "new_parser"))]
        assert_eq!(result.check.params, vec![1]);
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
    #[cfg(not(feature = "new_parser"))]
    fn test_return_rows() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE id = $2 RETURNING *")
            .unwrap()
            .unwrap();
        assert_eq!(result.insert.returnin_list_deparsed, Some("*".into()));

        let result =
            run_test("UPDATE sharded SET id = $1 WHERE id = $2 RETURNING id, email, random()")
                .unwrap()
                .unwrap();
        assert_eq!(
            result.insert.returnin_list_deparsed,
            Some("id, email, random()".into())
        );
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_res_targets_to_insert_res_targets_expr_branch() {
        // Test that expression assignments (non-simple values) are deparsed correctly
        // and stored as UpdateValue::Expr in the insert mapping.
        let result = run_test("UPDATE sharded SET id = $1, email = random() WHERE id = $2")
            .unwrap()
            .unwrap();

        // The id column should be UpdateValue::Value (simple parameter)
        let id_value = result.insert.mapping.get("id").unwrap();
        std::assert_matches!(id_value, UpdateValue::Value(_));

        // The email column should be UpdateValue::Expr with the deparsed expression
        let email_value = result.insert.mapping.get("email").unwrap();
        match email_value {
            UpdateValue::Expr(expr) => assert_eq!(expr, "random()"),
            _ => panic!("Expected UpdateValue::Expr for email"),
        }
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_res_targets_to_insert_res_targets_expr_arithmetic() {
        // Test arithmetic expressions are deparsed correctly
        let result = run_test("UPDATE sharded SET id = $1, counter = counter + 1 WHERE id = $2")
            .unwrap()
            .unwrap();

        let counter_value = result.insert.mapping.get("counter").unwrap();
        match counter_value {
            UpdateValue::Expr(expr) => assert_eq!(expr, "counter + 1"),
            _ => panic!("Expected UpdateValue::Expr for counter"),
        }
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_res_targets_to_insert_res_targets_expr_coalesce() {
        // Test COALESCE expressions are deparsed correctly
        let result =
            run_test("UPDATE sharded SET id = $1, name = COALESCE(name, 'default') WHERE id = $2")
                .unwrap()
                .unwrap();

        let name_value = result.insert.mapping.get("name").unwrap();
        match name_value {
            UpdateValue::Expr(expr) => assert_eq!(expr, "COALESCE(name, 'default')"),
            _ => panic!("Expected UpdateValue::Expr for name"),
        }
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
        ]);

        // Create a mock data row with values for columns not in the UPDATE SET clause
        let mut data_row = DataRow::new();
        data_row.add("1"); // id - will be overwritten by mapping
        data_row.add("old@example.com"); // email - will be overwritten by mapping
        data_row.add("other_value"); // other_col - from existing row

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

    const BIND_PARAM_PATTERN: &str = r#"(\$\d+)"#;

    fn bind_param_regex() -> Regex {
        Regex::new(BIND_PARAM_PATTERN).unwrap()
    }

    fn assert_equivalent_sql(actual: &str, expected: &str) {
        let pattern = bind_param_regex().replace_all(expected, "BIND_PARAM");
        let pattern = regex::escape(&pattern).replace("BIND_PARAM", BIND_PARAM_PATTERN);
        let regex = Regex::new(&pattern).unwrap();
        assert!(
            regex.is_match(actual),
            "Expected {:?} to match {:?}",
            actual,
            pattern
        )
    }

    fn assert_all_bind_params_used(original: &str, rewritten: &str) {
        for captures in bind_param_regex().captures_iter(original) {
            let param = &captures[1];
            assert!(
                rewritten.contains(param),
                "Expected {:?} to use param {}",
                rewritten,
                param
            );
        }
    }
}
