// Note: Most of these tests previously were in parser/rewrite/statement/update.rs

use crate::frontend::router::sharding::ShardedTable;
use indexmap::{IndexSet, indexset};
use pg_raw_parse::{Node, nodes};
use pgdog_config::{Rewrite, RewriteMode};

use crate::backend::ShardingSchema;
use crate::backend::{ShardedTables, replication::ShardedSchemas};
use crate::frontend::ClientRequest;
use crate::frontend::client::query_engine::QueryEngineContext;
use crate::frontend::client::query_engine::multi_step::error::Error;
use crate::frontend::client::query_engine::multi_step::types::{
    QueryPlanner, ResponseHistory, StatementSource, StepRequest, StepResponses,
};
use crate::frontend::client::test::TestClient;
use crate::frontend::router::parser::rewrite::statement::Error as RewriteError;
use crate::frontend::router::parser::{AstContext, Cache, Error as ParserError};
use crate::net::messages::row_description::Field;
use crate::net::{
    Bind, DataRow, Execute, Parameters, Parse, Query, RowDescription, Sync, bind::Parameter,
};

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

#[derive(Debug)]
struct Statement {
    stmt: String,
    params: IndexSet<u16>,
}

#[derive(Debug)]
struct TargetTable {
    name: String,
}

#[derive(Debug)]
struct ShardingKeyUpdate {
    query: String,
    delete: Statement,
    insert: Option<Box<dyn StatementSource>>,
}

impl ShardingKeyUpdate {
    fn with_update<R>(&self, f: impl FnOnce(&nodes::UpdateStmt) -> R) -> R {
        let stmt = pg_raw_parse::parse(&self.query).unwrap();
        match stmt.stmts().next().unwrap() {
            Node::UpdateStmt(stmt) => f(stmt),
            _ => panic!("Not an update"),
        }
    }

    fn is_returning(&self) -> bool {
        self.with_update(|update| update.returning_clause().is_some())
    }

    fn target_table(&self) -> TargetTable {
        self.with_update(|update| TargetTable {
            name: QueryPlanner::target_table(update).name.to_string(),
        })
    }

    fn sharded_table(&self, tables: &[ShardedTable]) -> Option<String> {
        self.with_update(|update| {
            QueryPlanner::sharded_table(tables, update).map(|table| table.column.to_string())
        })
    }
}

fn placeholders(sql: &str) -> u16 {
    sql.split('$')
        .skip(1)
        .filter_map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u16>()
                .ok()
        })
        .max()
        .unwrap_or_default()
}

fn bind_params(request: &ClientRequest) -> IndexSet<u16> {
    request
        .parameters()
        .unwrap()
        .map(|bind| {
            bind.params_raw()
                .iter()
                .map(|param| {
                    std::str::from_utf8(&param.data)
                        .unwrap()
                        .parse::<u16>()
                        .unwrap()
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn run_test_with(
    client: &mut TestClient,
    query: &str,
) -> Result<Option<ShardingKeyUpdate>, Error> {
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    let params = (1..=placeholders(query))
        .map(|number| Parameter::new(number.to_string().as_bytes()))
        .collect::<Vec<_>>();
    client.client.client_request = ClientRequest::from(vec![
        Parse::new_anonymous(query).into(),
        Bind::new_params("", &params).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let mut context = QueryEngineContext::new(&mut client.client);

    let ast = {
        let cluster = client.engine.backend.cluster()?;
        let ast_context = AstContext::from_cluster(cluster, context.params);
        let buffered = context.client_request.query()?.unwrap();
        Cache::get().query(&buffered, &ast_context, context.prepared_statements)?
    };
    context.client_request.ast = Some(ast);

    client.engine.route_query(&mut context, None).await?;
    client.engine.connect_transaction(&mut context).await?;

    let schema = client.engine.backend.cluster()?.sharding_schema();
    let request = context.client_request.clone();
    let Some(planner) =
        QueryPlanner::plan_sharding_key_update(&request, &mut client.engine, &mut context, schema)
            .await?
    else {
        return Ok(None);
    };

    let delete = planner.steps[0]
        .request
        .assemble(&ResponseHistory::default())?
        .expect("delete step resolves statically");
    let delete = Statement {
        stmt: delete.query()?.unwrap().query().to_string(),
        params: bind_params(&delete),
    };

    let insert = planner.steps.get(1).and_then(|step| match &step.request {
        StepRequest::Statement(statement) => Some(statement.source.clone()),
        _ => None,
    });

    Ok(Some(ShardingKeyUpdate {
        query: query.to_string(),
        delete,
        insert,
    }))
}

async fn run_test(query: &str) -> Result<Option<ShardingKeyUpdate>, Error> {
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    run_test_with(&mut client, query).await
}

#[tokio::test]
async fn test_select_basic_where_param() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
        .await
        .unwrap()
        .unwrap();

    // SELECT should have WHERE clause with param renumbered to $1
    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2]);

    let schema = default_schema();
    let tables = schema.tables.tables();
    assert_eq!(result.target_table().name, "sharded");
    assert_eq!(result.sharded_table(tables).unwrap(), "id");
}

#[tokio::test]
async fn test_select_multiple_where_params() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 AND name = $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
    assert!(!result.is_returning());
}

#[tokio::test]
async fn test_select_non_sequential_params() {
    // Params in WHERE are $3 and $5, should be renumbered to $1 and $2
    let result = run_test(
        "UPDATE sharded SET id = $1, value = $2, other = $4 WHERE email = $3 AND name = $5",
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 AND name = $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![3, 5]);
}

#[tokio::test]
async fn test_delete_basic() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 RETURNING *"
    );

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

#[tokio::test]
async fn test_no_params_in_where() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = 'test@example.com'")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = 'test@example.com' RETURNING *"
    );
    assert!(result.delete.params.is_empty());
}

#[tokio::test]
async fn test_where_with_in_clause() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $4)")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email IN ($1, $2, $3) RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3, 4]);
}

#[tokio::test]
async fn test_where_with_comparison_operators() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE count > $2 AND count < $3")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE count > $1 AND count < $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
}

#[tokio::test]
async fn test_where_with_or_condition() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $3")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 OR name = $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
}

#[tokio::test]
async fn test_high_param_numbers() {
    let result = run_test("UPDATE sharded SET id = $10 WHERE email = $20 AND name = $30")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 AND name = $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![20, 30]);
}

#[tokio::test]
async fn test_non_sharding_key_update_returns_none() {
    // Updating a non-sharding column should return None
    let result = run_test("UPDATE sharded SET email = $1 WHERE id = $2")
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_where_with_like() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email LIKE $2")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email LIKE $1 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2]);
}

#[tokio::test]
async fn test_where_with_is_null() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND deleted_at IS NULL")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 AND deleted_at IS NULL RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2]);
}

#[tokio::test]
async fn test_where_with_between() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE created_at BETWEEN $2 AND $3")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE created_at BETWEEN $1 AND $2 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
}

#[tokio::test]
async fn test_same_param_used_twice() {
    // Same parameter $2 used twice in WHERE clause
    let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2")
        .await
        .unwrap()
        .unwrap();

    // Both occurrences should be renumbered to $1
    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email = $1 OR name = $1 RETURNING *"
    );
    // Only one unique param in the mapping
    assert_eq!(result.delete.params, indexset![2]);
}

#[tokio::test]
async fn test_same_param_used_multiple_times() {
    // $2 used three times
    let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $2 AND c = $2")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE a = $1 AND b = $1 AND c = $1 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2]);
}

#[tokio::test]
async fn test_mixed_repeated_and_unique_params() {
    // $2 used twice, $3 used once
    let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $3 AND c = $2")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE a = $1 AND b = $2 AND c = $1 RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
}

#[tokio::test]
async fn test_repeated_params_in_in_clause() {
    // Same param repeated in IN clause (unusual but valid)
    let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $2)")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        result.delete.stmt,
        "DELETE FROM sharded WHERE email IN ($1, $2, $1) RETURNING *"
    );
    assert_eq!(result.delete.params, indexset![2, 3]);
}

#[tokio::test]
async fn test_sharding_key_not_changed() {
    let result = run_test("UPDATE sharded SET id = $1 WHERE id = $1 AND email = $2")
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_unsupported_assignment() {
    let result = run_test("UPDATE sharded SET id = random() WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = random()"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_arithmetic_add() {
    let result = run_test("UPDATE sharded SET id = id + 1 WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = id + 1"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_arithmetic_multiply() {
    let result = run_test("UPDATE sharded SET id = id * 2 WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = id * 2"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_arithmetic_with_param() {
    let result = run_test("UPDATE sharded SET id = id + $2 WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = id + $2"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_now() {
    let result = run_test("UPDATE sharded SET id = now() WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = now()"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_coalesce() {
    let result = run_test("UPDATE sharded SET id = coalesce(id, 0) WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = COALESCE(id, 0)"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_case() {
    let result =
        run_test("UPDATE sharded SET id = CASE WHEN id > 0 THEN 1 ELSE 0 END WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = CASE WHEN id > 0 THEN 1 ELSE 0 END"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_subquery() {
    let result =
        run_test("UPDATE sharded SET id = (SELECT max(id) FROM sharded) WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = (SELECT max(id) FROM sharded)"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_column_reference() {
    let result = run_test("UPDATE sharded SET id = other_column WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = other_column"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_concat() {
    let result = run_test("UPDATE sharded SET id = id || '_suffix' WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = id || '_suffix'"
    );
}

#[tokio::test]
async fn test_unsupported_assignment_negation() {
    let result = run_test("UPDATE sharded SET id = -id WHERE id = $1").await;
    std::assert_matches!(
        result,
        Err(Error::Parser(ParserError::Rewrite(RewriteError::UnsupportedShardingKeyUpdate(msg)))) if msg == "\"id\" = - id"
    );
}

#[tokio::test]
async fn test_insert_build_request_with_expr_column() {
    // Test that INSERT statement is built correctly when there are expression columns.
    // The expression should appear directly in the VALUES clause.
    // Use literal values (not placeholders) to avoid needing bind parameters.
    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    let old_id = client.random_id_for_shard(0);
    let new_id = client.random_id_for_shard(1);
    let result = run_test_with(
        &mut client,
        &format!("UPDATE sharded SET id = {new_id}, value = random() WHERE id = {old_id}"),
    )
    .await
    .unwrap()
    .unwrap();

    // Create a mock row description matching the SELECT * result
    let row_description = RowDescription::new(&[
        Field::bigint("id"),
        Field::text("value"),
        Field::text("other_col"),
        Field::text("other_other_col"),
    ]);

    // Create a mock data row with values for columns not in the UPDATE SET clause
    let mut data_row = DataRow::new();
    data_row.add("1"); // id - will be overwritten by mapping
    data_row.add("old_value"); // value - will be overwritten by mapping
    data_row.add("other_value"); // other_col - from existing row
    data_row.add("other_other_value"); // other_other_col - from existing row

    // The INSERT is built from the DELETE step's response.
    let mut map = ResponseHistory::default();
    map.push(StepResponses {
        key: Some("delete"),
        row_description: Some(row_description),
        rows: vec![data_row],
        ..Default::default()
    });

    let stmt = result
        .insert
        .expect("insert step should exist")
        .resolve(&map)
        .unwrap()
        .expect("statement resolves")
        .0
        .query()
        .to_string();

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
    let placeholders = placeholders(&stmt);
    assert_eq!(
        placeholders, 2,
        "one placeholder per non-inlined column: {stmt}"
    );
    assert!(
        (1..=placeholders).all(|number| stmt.contains(&format!("${number}"))),
        "Parameter numbering should be sequential without gaps: {}",
        stmt
    );
}
