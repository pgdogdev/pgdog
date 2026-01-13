// use std::collections::HashMap;

// use super::*;
// use crate::{
//     backend::pool::{connection::binding::Binding, Guard, Request},
//     frontend::router::{
//         self as router,
//         parser::{
//             self as parser,
//             rewrite::{AssignmentValue, ShardKeyRewritePlan},
//             Shard,
//         },
//     },
//     net::messages::Protocol,
//     net::{
//         messages::{
//             bind::Format, command_complete::CommandComplete, Bind, DataRow, FromBytes, Message,
//             RowDescription, ToBytes,
//         },
//         ErrorResponse, ReadyForQuery,
//     },
//     util::escape_identifier,
// };
// use pgdog_plugin::pg_query::NodeEnum;
// use tracing::warn;

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::frontend::router::{
//         parser::{rewrite::Assignment, route::Shard, table::OwnedTable, ShardWithPriority},
//         Route,
//     };
//     use crate::{
//         backend::{
//             databases::{self, databases, lock, User as DbUser},
//             pool::{cluster::Cluster, Request},
//         },
//         config::{
//             self,
//             core::ConfigAndUsers,
//             database::Database,
//             sharding::{DataType, FlexibleType, ShardedMapping, ShardedMappingKind, ShardedTable},
//             users::User as ConfigUser,
//             RewriteMode,
//         },
//         frontend::Client,
//         net::{Query, Stream},
//     };
//     use std::collections::HashSet;

//     async fn configure_cluster(two_pc_enabled: bool) -> Cluster {
//         let mut cfg = ConfigAndUsers::default();
//         cfg.config.general.two_phase_commit = two_pc_enabled;
//         cfg.config.general.two_phase_commit_auto = Some(false);
//         cfg.config.rewrite.enabled = true;
//         cfg.config.rewrite.shard_key = RewriteMode::Rewrite;

//         cfg.config.databases = vec![
//             Database {
//                 name: "pgdog_sharded".into(),
//                 host: "127.0.0.1".into(),
//                 port: 5432,
//                 database_name: Some("pgdog".into()),
//                 shard: 0,
//                 ..Default::default()
//             },
//             Database {
//                 name: "pgdog_sharded".into(),
//                 host: "127.0.0.1".into(),
//                 port: 5432,
//                 database_name: Some("pgdog".into()),
//                 shard: 1,
//                 ..Default::default()
//             },
//         ];

//         cfg.config.sharded_tables = vec![ShardedTable {
//             database: "pgdog_sharded".into(),
//             name: Some("sharded".into()),
//             column: "id".into(),
//             data_type: DataType::Bigint,
//             primary: true,
//             ..Default::default()
//         }];

//         let shard0_values = HashSet::from([
//             FlexibleType::Integer(1),
//             FlexibleType::Integer(2),
//             FlexibleType::Integer(3),
//             FlexibleType::Integer(4),
//         ]);
//         let shard1_values = HashSet::from([
//             FlexibleType::Integer(5),
//             FlexibleType::Integer(6),
//             FlexibleType::Integer(7),
//         ]);

//         cfg.config.sharded_mappings = vec![
//             ShardedMapping {
//                 database: "pgdog_sharded".into(),
//                 table: Some("sharded".into()),
//                 column: "id".into(),
//                 kind: ShardedMappingKind::List,
//                 values: shard0_values,
//                 shard: 0,
//                 ..Default::default()
//             },
//             ShardedMapping {
//                 database: "pgdog_sharded".into(),
//                 table: Some("sharded".into()),
//                 column: "id".into(),
//                 kind: ShardedMappingKind::List,
//                 values: shard1_values,
//                 shard: 1,
//                 ..Default::default()
//             },
//         ];

//         cfg.users.users = vec![ConfigUser {
//             name: "pgdog".into(),
//             database: "pgdog_sharded".into(),
//             password: Some("pgdog".into()),
//             two_phase_commit: Some(two_pc_enabled),
//             two_phase_commit_auto: Some(false),
//             ..Default::default()
//         }];

//         config::set(cfg).unwrap();
//         databases::init().unwrap();

//         let user = DbUser {
//             user: "pgdog".into(),
//             database: "pgdog_sharded".into(),
//         };

//         databases()
//             .all()
//             .get(&user)
//             .expect("cluster missing")
//             .clone()
//     }

//     async fn prepare_table(cluster: &Cluster) {
//         let request = Request::default();
//         let mut primary = cluster.primary(0, &request).await.unwrap();
//         primary
//             .execute("CREATE TABLE IF NOT EXISTS sharded (id BIGINT PRIMARY KEY, value TEXT)")
//             .await
//             .unwrap();
//         primary.execute("TRUNCATE TABLE sharded").await.unwrap();
//         primary
//             .execute("INSERT INTO sharded (id, value) VALUES (1, 'old')")
//             .await
//             .unwrap();
//     }

//     async fn table_state(cluster: &Cluster) -> (i64, i64) {
//         let request = Request::default();
//         let mut primary = cluster.primary(0, &request).await.unwrap();
//         let old_id = primary
//             .fetch_all::<i64>("SELECT COUNT(*)::bigint FROM sharded WHERE id = 1")
//             .await
//             .unwrap()[0];
//         let new_id = primary
//             .fetch_all::<i64>("SELECT COUNT(*)::bigint FROM sharded WHERE id = 5")
//             .await
//             .unwrap()[0];
//         (old_id, new_id)
//     }

//     fn new_client() -> Client {
//         let stream = Stream::dev_null();
//         let mut client = Client::new_test(stream, Parameters::default());
//         client.params.insert("database", "pgdog_sharded");
//         client.connect_params.insert("database", "pgdog_sharded");
//         client
//     }

//     #[tokio::test]
//     async fn shard_key_rewrite_moves_row_between_shards() {
//         crate::logger();
//         let _lock = lock();

//         let cluster = configure_cluster(true).await;
//         prepare_table(&cluster).await;

//         let mut client = new_client();
//         client
//             .client_request
//             .messages
//             .push(Query::new("UPDATE sharded SET id = 5 WHERE id = 1").into());

//         let mut engine = QueryEngine::from_client(&client).unwrap();
//         let mut context = QueryEngineContext::new(&mut client);

//         engine.handle(&mut context).await.unwrap();

//         let (old_count, new_count) = table_state(&cluster).await;
//         assert_eq!(old_count, 0, "old row must be removed");
//         assert_eq!(
//             new_count, 1,
//             "new row must be inserted on destination shard"
//         );

//         databases::shutdown();
//         config::load_test();
//     }

//     #[test]
//     fn build_delete_sql_requires_where_clause() {
//         let parsed = pgdog_plugin::pg_query::parse("UPDATE sharded SET id = 5")
//             .expect("parse update without where");
//         let stmt = parsed
//             .protobuf
//             .stmts
//             .first()
//             .and_then(|node| node.stmt.as_ref())
//             .and_then(|node| node.node.as_ref())
//             .expect("statement node");

//         let mut update_stmt = match stmt {
//             NodeEnum::UpdateStmt(update) => (**update).clone(),
//             _ => panic!("expected update statement"),
//         };

//         update_stmt.where_clause = None;

//         let plan = ShardKeyRewritePlan::new(
//             OwnedTable {
//                 name: "sharded".into(),
//                 schema: None,
//                 alias: None,
//             },
//             Route::write(ShardWithPriority::new_default_unset(Shard::Direct(0))),
//             Some(1),
//             update_stmt,
//             vec![Assignment::new("id".into(), AssignmentValue::Integer(5))],
//         );

//         let err = build_delete_sql(&plan).expect_err("expected invariant error");
//         match err {
//             Error::Router(router::Error::Parser(parser::Error::ShardKeyRewriteInvariant {
//                 reason,
//             })) => {
//                 assert!(
//                     reason.contains("without WHERE clause"),
//                     "unexpected reason: {}",
//                     reason
//                 );
//             }
//             other => panic!("unexpected error variant: {other:?}"),
//         }
//     }
// }
