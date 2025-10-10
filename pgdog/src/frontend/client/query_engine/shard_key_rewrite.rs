use std::collections::HashMap;

use super::*;
use crate::{
    backend::pool::{Guard, Request},
    frontend::router::{
        self as router,
        parser::{
            self as parser,
            rewrite::{AssignmentValue, ShardKeyRewritePlan},
        },
    },
    net::{
        messages::{
            bind::Format, command_complete::CommandComplete, Bind, DataRow, FromBytes, Message,
            RowDescription, ToBytes,
        },
        ErrorResponse, Protocol, ReadyForQuery,
    },
    util::escape_identifier,
};
use pgdog_plugin::pg_query::NodeEnum;

impl QueryEngine {
    pub(super) async fn shard_key_rewrite(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        plan: ShardKeyRewritePlan,
    ) -> Result<(), Error> {
        let cluster = self.backend.cluster()?.clone();

        if !cluster.two_pc_enabled() {
            let mut error = ErrorResponse::default();
            error.code = "0A000".into();
            error.message =
                "rewrite_shard_key_updates requires two_phase_commit to be enabled".into();
            let bytes_sent = context
                .stream
                .error(error, context.in_transaction())
                .await?;
            self.stats.sent(bytes_sent);
            return Ok(());
        }

        let source_shard = match plan.route().shard() {
            Shard::Direct(value) => *value,
            _ => {
                return Err(Error::Router(router::Error::Parser(
                    parser::Error::EmptyQuery,
                )))
            }
        };

        let Some(target_shard) = plan.new_shard() else {
            return self.execute(context, plan.route()).await;
        };

        if source_shard == target_shard {
            return self.execute(context, plan.route()).await;
        }

        let request = Request::default();
        let mut source = cluster
            .primary(source_shard, &request)
            .await
            .map_err(|err| Error::Router(router::Error::Pool(err)))?;
        let mut target = cluster
            .primary(target_shard, &request)
            .await
            .map_err(|err| Error::Router(router::Error::Pool(err)))?;

        source.execute("BEGIN").await?;
        target.execute("BEGIN").await?;

        let delete_sql = build_delete_sql(&plan)?;
        let mut delete = execute_sql(&mut source, &delete_sql).await?;

        let deleted_rows = delete
            .command_complete
            .rows()
            .unwrap_or_default()
            .unwrap_or_default();

        if deleted_rows == 0 {
            source.execute("ROLLBACK").await?;
            target.execute("ROLLBACK").await?;
            return self.send_update_complete(context, 0).await;
        }

        let row_description = delete
            .row_description
            .take()
            .ok_or_else(|| Error::Router(router::Error::Parser(parser::Error::EmptyQuery)))?;
        let data_row = delete
            .data_row
            .take()
            .ok_or_else(|| Error::Router(router::Error::Parser(parser::Error::EmptyQuery)))?;

        let parameters = context.client_request.parameters()?;
        let assignments = apply_assignments(&row_description, &data_row, &plan, parameters)?;
        let insert_sql = build_insert_sql(&plan, &row_description, &assignments);

        execute_sql(&mut target, &insert_sql).await?;

        let identifier = cluster.identifier();
        let transaction_name = self.two_pc.transaction().to_string();
        let guard_phase_one = self.two_pc.phase_one(&identifier).await?;

        let prepare_source = format!(
            "PREPARE TRANSACTION '{}_{}'",
            transaction_name, source_shard
        );
        let prepare_target = format!(
            "PREPARE TRANSACTION '{}_{}'",
            transaction_name, target_shard
        );
        execute_sql(&mut source, &prepare_source).await?;
        execute_sql(&mut target, &prepare_target).await?;

        let guard_phase_two = self.two_pc.phase_two(&identifier).await?;
        let commit_source = format!("COMMIT PREPARED '{}_{}'", transaction_name, source_shard);
        let commit_target = format!("COMMIT PREPARED '{}_{}'", transaction_name, target_shard);
        execute_sql(&mut source, &commit_source).await?;
        execute_sql(&mut target, &commit_target).await?;

        self.two_pc.done().await?;

        drop(guard_phase_two);
        drop(guard_phase_one);

        self.send_update_complete(context, deleted_rows).await
    }

    async fn send_update_complete(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        rows: usize,
    ) -> Result<(), Error> {
        let command = if rows == 1 {
            CommandComplete::from_str("UPDATE 1")
        } else {
            CommandComplete::from_str(&format!("UPDATE {}", rows))
        };

        let bytes_sent = context
            .stream
            .send_many(&[
                command.message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;
        self.stats.sent(bytes_sent);
        Ok(())
    }
}

struct SqlResult {
    row_description: Option<RowDescription>,
    data_row: Option<DataRow>,
    command_complete: CommandComplete,
}

async fn execute_sql(server: &mut Guard, sql: &str) -> Result<SqlResult, Error> {
    let messages = server.execute(sql).await?;
    parse_messages(messages)
}

fn parse_messages(messages: Vec<Message>) -> Result<SqlResult, Error> {
    let mut row_description = None;
    let mut data_row = None;
    let mut command_complete = None;

    for message in messages {
        match message.code() {
            'T' => {
                let rd = RowDescription::from_bytes(message.to_bytes()?)?;
                row_description = Some(rd);
            }
            'D' => {
                let row = DataRow::from_bytes(message.to_bytes()?)?;
                data_row = Some(row);
            }
            'C' => {
                let cc = CommandComplete::from_bytes(message.to_bytes()?)?;
                command_complete = Some(cc);
            }
            _ => (),
        }
    }

    let command_complete = command_complete
        .ok_or_else(|| Error::Router(router::Error::Parser(parser::Error::EmptyQuery)))?;

    Ok(SqlResult {
        row_description,
        data_row,
        command_complete,
    })
}

fn build_delete_sql(plan: &ShardKeyRewritePlan) -> Result<String, Error> {
    let mut sql = format!("DELETE FROM {}", plan.table());
    if let Some(where_clause) = plan.statement().where_clause.as_ref() {
        match where_clause.deparse() {
            Ok(where_sql) => {
                sql.push_str(" WHERE ");
                sql.push_str(&where_sql);
            }
            Err(_) => {
                let update_sql = NodeEnum::UpdateStmt(Box::new(plan.statement().clone()))
                    .deparse()
                    .map_err(|err| {
                        Error::Router(router::Error::Parser(parser::Error::PgQuery(err)))
                    })?;
                if let Some(index) = update_sql.to_uppercase().find(" WHERE ") {
                    sql.push_str(&update_sql[index..]);
                } else {
                    return Err(Error::Router(router::Error::Parser(
                        parser::Error::EmptyQuery,
                    )));
                }
            }
        }
    }
    sql.push_str(" RETURNING *");
    Ok(sql)
}

fn build_insert_sql(
    plan: &ShardKeyRewritePlan,
    row_description: &RowDescription,
    assignments: &[Option<String>],
) -> String {
    let mut columns = Vec::with_capacity(row_description.fields.len());
    let mut values = Vec::with_capacity(row_description.fields.len());

    for (index, field) in row_description.fields.iter().enumerate() {
        columns.push(format!("\"{}\"", escape_identifier(&field.name)));
        match &assignments[index] {
            Some(value) => values.push(format_literal(value)),
            None => values.push("NULL".into()),
        }
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        plan.table(),
        columns.join(", "),
        values.join(", ")
    )
}

fn apply_assignments(
    row_description: &RowDescription,
    data_row: &DataRow,
    plan: &ShardKeyRewritePlan,
    parameters: Option<&Bind>,
) -> Result<Vec<Option<String>>, Error> {
    let mut values: Vec<Option<String>> = (0..row_description.fields.len())
        .map(|index| data_row.get_text(index).map(|value| value.to_owned()))
        .collect();

    let mut column_map = HashMap::new();
    for (index, field) in row_description.fields.iter().enumerate() {
        column_map.insert(field.name.to_lowercase(), index);
    }

    for assignment in plan.assignments() {
        let column_index = column_map
            .get(&assignment.column().to_lowercase())
            .ok_or_else(|| Error::Router(router::Error::Parser(parser::Error::ColumnNoTable)))?;

        let new_value = match assignment.value() {
            AssignmentValue::Integer(value) => Some(value.to_string()),
            AssignmentValue::String(value) => Some(value.clone()),
            AssignmentValue::Boolean(value) => Some(value.to_string()),
            AssignmentValue::Null => None,
            AssignmentValue::Parameter(index) => {
                let bind = parameters.ok_or_else(|| {
                    Error::Router(router::Error::Parser(parser::Error::MissingParameter(
                        *index as usize,
                    )))
                })?;
                if *index <= 0 {
                    return Err(Error::Router(router::Error::Parser(
                        parser::Error::MissingParameter(0),
                    )));
                }
                let param_index = (*index as usize) - 1;
                let value = bind.parameter(param_index)?.ok_or_else(|| {
                    Error::Router(router::Error::Parser(parser::Error::MissingParameter(
                        *index as usize,
                    )))
                })?;
                let text = match value.format() {
                    Format::Text => value.text().map(|text| text.to_owned()),
                    Format::Binary => value.text().map(|text| text.to_owned()),
                };
                Some(text.ok_or_else(|| {
                    Error::Router(router::Error::Parser(parser::Error::MissingParameter(
                        *index as usize,
                    )))
                })?)
            }
            AssignmentValue::Column(column) => {
                let reference = column_map.get(&column.to_lowercase()).ok_or_else(|| {
                    Error::Router(router::Error::Parser(parser::Error::ColumnNoTable))
                })?;
                values[*reference].clone()
            }
        };

        values[*column_index] = new_value;
    }

    Ok(values)
}

fn format_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{}'", escaped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{
            databases::{self, databases, lock, User as DbUser},
            pool::{cluster::Cluster, Request},
        },
        config::{
            self,
            core::ConfigAndUsers,
            database::Database,
            general::ShardKeyUpdateMode,
            sharding::{DataType, FlexibleType, ShardedMapping, ShardedMappingKind, ShardedTable},
            users::User as ConfigUser,
        },
        frontend::Client,
        net::{Query, Stream},
    };
    use std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    async fn configure_cluster(two_pc_enabled: bool) -> Cluster {
        let mut cfg = ConfigAndUsers::default();
        cfg.config.general.two_phase_commit = two_pc_enabled;
        cfg.config.general.two_phase_commit_auto = Some(false);
        cfg.config.general.rewrite_shard_key_updates = ShardKeyUpdateMode::Rewrite;

        cfg.config.databases = vec![
            Database {
                name: "pgdog_sharded".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: Some("pgdog".into()),
                shard: 0,
                ..Default::default()
            },
            Database {
                name: "pgdog_sharded".into(),
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: Some("pgdog".into()),
                shard: 1,
                ..Default::default()
            },
        ];

        cfg.config.sharded_tables = vec![ShardedTable {
            database: "pgdog_sharded".into(),
            name: Some("sharded".into()),
            column: "id".into(),
            data_type: DataType::Bigint,
            primary: true,
            ..Default::default()
        }];

        let shard0_values = HashSet::from([
            FlexibleType::Integer(1),
            FlexibleType::Integer(2),
            FlexibleType::Integer(3),
            FlexibleType::Integer(4),
        ]);
        let shard1_values = HashSet::from([
            FlexibleType::Integer(5),
            FlexibleType::Integer(6),
            FlexibleType::Integer(7),
        ]);

        cfg.config.sharded_mappings = vec![
            ShardedMapping {
                database: "pgdog_sharded".into(),
                table: Some("sharded".into()),
                column: "id".into(),
                kind: ShardedMappingKind::List,
                values: shard0_values,
                shard: 0,
                ..Default::default()
            },
            ShardedMapping {
                database: "pgdog_sharded".into(),
                table: Some("sharded".into()),
                column: "id".into(),
                kind: ShardedMappingKind::List,
                values: shard1_values,
                shard: 1,
                ..Default::default()
            },
        ];

        cfg.users.users = vec![ConfigUser {
            name: "pgdog".into(),
            database: "pgdog_sharded".into(),
            password: Some("pgdog".into()),
            two_phase_commit: Some(two_pc_enabled),
            two_phase_commit_auto: Some(false),
            ..Default::default()
        }];

        config::set(cfg).unwrap();
        databases::init();

        let user = DbUser {
            user: "pgdog".into(),
            database: "pgdog_sharded".into(),
        };

        databases()
            .all()
            .get(&user)
            .expect("cluster missing")
            .clone()
    }

    async fn prepare_table(cluster: &Cluster) {
        let request = Request::default();
        let mut primary = cluster.primary(0, &request).await.unwrap();
        primary
            .execute("CREATE TABLE IF NOT EXISTS sharded (id BIGINT PRIMARY KEY, value TEXT)")
            .await
            .unwrap();
        primary.execute("TRUNCATE TABLE sharded").await.unwrap();
        primary
            .execute("INSERT INTO sharded (id, value) VALUES (1, 'old')")
            .await
            .unwrap();
    }

    async fn table_state(cluster: &Cluster) -> (i64, i64) {
        let request = Request::default();
        let mut primary = cluster.primary(0, &request).await.unwrap();
        let old_id = primary
            .fetch_all::<i64>("SELECT COUNT(*)::bigint FROM sharded WHERE id = 1")
            .await
            .unwrap()[0];
        let new_id = primary
            .fetch_all::<i64>("SELECT COUNT(*)::bigint FROM sharded WHERE id = 5")
            .await
            .unwrap()[0];
        (old_id, new_id)
    }

    fn new_client() -> Client {
        let stream = Stream::DevNull;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5432);
        let mut client = Client::new_test(stream, addr);
        client.params.insert("database", "pgdog_sharded");
        client.connect_params.insert("database", "pgdog_sharded");
        client
    }

    #[tokio::test]
    async fn shard_key_rewrite_moves_row_between_shards() {
        crate::logger();
        let _lock = lock();

        let cluster = configure_cluster(true).await;
        prepare_table(&cluster).await;

        let mut client = new_client();
        client
            .client_request
            .messages
            .push(Query::new("UPDATE sharded SET id = 5 WHERE id = 1").into());

        let mut engine = QueryEngine::from_client(&client).unwrap();
        let mut context = QueryEngineContext::new(&mut client);

        engine.handle(&mut context).await.unwrap();

        let (old_count, new_count) = table_state(&cluster).await;
        assert_eq!(old_count, 0, "old row must be removed");
        assert_eq!(
            new_count, 1,
            "new row must be inserted on destination shard"
        );

        databases::shutdown();
        config::load_test();
    }
}
