use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use once_cell::sync::Lazy;
use pg_query::{
    protobuf::{InsertStmt, ParseResult},
    NodeEnum,
};

use super::super::{Error, Table};
use crate::{
    backend::{Cluster, Server, ShardingSchema},
    config::Role,
    frontend::router::parser::{Insert, Shard},
    net::{
        replication::{xlog_data::XLogPayload, Relation, XLogData},
        CopyData, ErrorResponse, Execute, Flush, FromBytes, Parse, Protocol, Sync, ToBytes,
    },
    state::State,
};

static STATEMENT_COUNTER: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(1));
fn statement_name() -> String {
    format!(
        "__pgdog_repl_{}",
        STATEMENT_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
struct Key {
    schema: String,
    name: String,
}

#[derive(Default, Debug, Clone)]
struct Statements {
    insert: Statement,
    upsert: Statement,
    update: Statement,
}

#[derive(Default, Debug, Clone)]
struct Statement {
    name: String,
    query: String,
    ast: ParseResult,
}

impl Statement {
    fn parse(&self) -> Parse {
        Parse::named(&self.name, &self.query)
    }

    fn new(query: &str) -> Result<Self, Error> {
        let ast = pg_query::parse(query)?.protobuf;
        Ok(Self {
            name: statement_name(),
            query: query.to_string(),
            ast,
        })
    }

    fn insert(&self) -> Option<&Box<InsertStmt>> {
        self.ast
            .stmts
            .first()
            .map(|stmt| {
                stmt.stmt.as_ref().map(|stmt| {
                    stmt.node.as_ref().map(|node| {
                        if let NodeEnum::InsertStmt(ref insert) = node {
                            Some(insert)
                        } else {
                            None
                        }
                    })
                })
            })
            .flatten()
            .flatten()
            .flatten()
    }
}

#[derive(Debug)]
pub struct StreamSubscriber {
    /// Destination cluster.
    cluster: Cluster,

    /// Sharding schema.
    sharding_schema: ShardingSchema,

    // Relation markers sent by the publisher.
    // Happens once per connection.
    relations: HashMap<i32, Relation>,

    // Tables in the publication on the publisher.
    tables: HashMap<Key, Table>,

    // Statements
    statements: HashMap<i32, Statements>,

    // Connections to shards.
    connections: Vec<Server>,
}

impl StreamSubscriber {
    pub fn new(cluster: &Cluster, tables: &[Table]) -> Self {
        Self {
            cluster: cluster.clone(),
            sharding_schema: cluster.sharding_schema(),
            relations: HashMap::new(),
            statements: HashMap::new(),
            tables: tables
                .into_iter()
                .map(|table| {
                    (
                        Key {
                            schema: table.table.schema.clone(),
                            name: table.table.name.clone(),
                        },
                        table.clone(),
                    )
                })
                .collect(),
            connections: vec![],
        }
    }

    /// Connect to all the shards.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut conns = vec![];

        for shard in self.cluster.shards() {
            let primary = shard
                .pools_with_roles()
                .iter()
                .filter(|(r, p)| r == &Role::Primary)
                .next()
                .ok_or(Error::NoPrimary)?
                .1
                .standalone()
                .await?;
            conns.push(primary);
        }

        self.connections = conns;
        Ok(())
    }

    pub async fn handle(&mut self, data: CopyData) -> Result<(), Error> {
        if self.connections.is_empty() {
            self.connect().await?;
        }

        if let Some(xlog) = data.xlog_data() {
            if let Some(payload) = xlog.payload() {
                match payload {
                    XLogPayload::Insert(insert) => {
                        let statements =
                            self.statements.get(&insert.oid).ok_or(Error::MissingData)?;
                        let bind = insert.tuple_data.to_bind(&statements.insert.name);
                        if let Some(insert) = statements.insert.insert() {
                            let insert = Insert::new(insert);
                            let val = insert.shard(&self.sharding_schema, Some(&bind))?;

                            for (shard, conn) in self.connections.iter_mut().enumerate() {
                                // Skip shards that don't match.
                                match val {
                                    Shard::Direct(direct) => {
                                        if direct != shard {
                                            continue;
                                        }
                                    }
                                    Shard::Multi(ref multi) => {
                                        if !multi.contains(&shard) {
                                            continue;
                                        }
                                    }
                                    _ => (),
                                }

                                conn.send(
                                    &vec![bind.clone().into(), Execute::new().into(), Sync.into()]
                                        .into(),
                                )
                                .await?;

                                for _ in 0..3 {
                                    let msg = conn.read().await?;
                                    match msg.code() {
                                        '2' | 'C' | 'Z' => (),
                                        'E' => {
                                            return Err(Error::PgError(ErrorResponse::from_bytes(
                                                msg.to_bytes()?,
                                            )?))
                                        }
                                        c => return Err(Error::OutOfSync(c)),
                                    }
                                }
                            }
                        }
                    }
                    XLogPayload::Relation(relation) => {
                        let table = self.tables.get(&Key {
                            schema: relation.namespace.clone(),
                            name: relation.name.clone(),
                        });

                        if let Some(table) = table {
                            let insert = Statement::new(&table.insert(false))?;
                            let upsert = Statement::new(&table.insert(true))?;
                            let begin = Parse::named("__pgdog_repl_begin", "BEGIN");
                            let commit = Parse::named("__pgdog_repl_commit", "COMMIT");

                            for server in &mut self.connections {
                                server
                                    .send(
                                        &vec![
                                            insert.parse().into(),
                                            upsert.parse().into(),
                                            begin.clone().into(),
                                            commit.clone().into(),
                                            Flush.into(),
                                        ]
                                        .into(),
                                    )
                                    .await?;
                                for _ in 0..4 {
                                    let reply = server.read().await?;
                                    match reply.code() {
                                        '1' => (),
                                        'E' => {
                                            return Err(Error::PgError(ErrorResponse::from_bytes(
                                                reply.to_bytes()?,
                                            )?))
                                        }
                                        c => return Err(Error::OutOfSync(c)),
                                    }
                                }
                            }

                            self.statements.insert(
                                relation.oid,
                                Statements {
                                    insert,
                                    upsert,
                                    update: Statement::default(),
                                },
                            );
                        }

                        self.relations.insert(relation.oid, relation);
                    }
                    _ => (),
                }
            }
        }

        Ok(())
    }
}
