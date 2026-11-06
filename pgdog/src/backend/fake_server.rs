use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{Bytes, BytesMut};
use pg_raw_parse::{ConstValue, Node, nodes};
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder, Runtime};

use crate::net::{
    Authentication, BackendKeyData, BindComplete, CloseComplete, CommandComplete, DataRow, Error,
    ErrorResponse, Field, FromBytes, NoData, ParameterDescription, ParameterStatus, ParseComplete,
    Protocol, ProtocolMessage, ReadyForQuery, RowDescription, Startup, Stream, ToBytes,
};

pub(crate) const HOST: &str = "fake-server";

const ROWS_PER_SHARD: usize = 10;

const TABLE_COLUMNS: &[(&str, &[&str])] = &[
    ("sharded", &["id", "value"]),
    ("sharded_omni", &["id", "value"]),
    ("sharded_lookup", &["org_id", "value"]),
];

const TERMINATE: char = 'X';

// When we count instructions, we run the fake servers on a separate thread, so that the counts of the benchmark thread only include PgDog's work.
// When we measure time, we run the fake servers on the benchmark thread, because handing replies between two threads would make the benchmark thread sleep
// and wake up through the kernel, (thus, the wake-ups would dominate the times)
static SEPARATE_THREAD: AtomicBool = AtomicBool::new(false);

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("fake-server")
        .enable_all()
        .build()
        .expect("fake server runtime")
});

#[cfg_attr(not(all(feature = "bench", target_os = "linux")), allow(dead_code))]
pub(crate) fn run_on_separate_thread() {
    SEPARATE_THREAD.store(true, Ordering::Relaxed);
}

pub(crate) fn connect() -> Stream {
    let (pgdog, server) = tokio::io::duplex(64 * 1024);
    let server = serve(Stream::memory(server));

    if SEPARATE_THREAD.load(Ordering::Relaxed) {
        RUNTIME.spawn(server);
    } else {
        tokio::spawn(server);
    }

    Stream::memory(pgdog)
}

async fn serve(mut stream: Stream) -> Result<(), Error> {
    handshake(&mut stream).await?;

    let mut replies: HashMap<String, Reply> = HashMap::new();
    let mut prepared_sql: HashMap<String, String> = HashMap::new();
    let mut bound_sql = String::new();
    let mut in_transaction = false;

    // If we get an error, Postgres ignores extended protocol messages until Sync (so we do the same)
    let mut skip_until_sync = false;

    loop {
        let message = ProtocolMessage::from_bytes(stream.read().await?.to_bytes())?;

        if skip_until_sync && !matches!(message, ProtocolMessage::Sync(_)) {
            continue;
        }

        match message {
            ProtocolMessage::Query(query) => {
                prepared_sql.remove("");

                let reply = cached_reply(&mut replies, query.query());
                if let Some(row_description) = &reply.row_description {
                    stream.write_all(row_description).await?;
                }
                reply.send_rows(&mut stream).await?;

                in_transaction = reply.starts_transaction.unwrap_or(in_transaction);
                stream
                    .send_flush(&ReadyForQuery::in_transaction(in_transaction))
                    .await?;
            }

            ProtocolMessage::Parse(parse) => {
                prepared_sql.insert(parse.name().to_owned(), parse.query().to_owned());
                stream.send(&ParseComplete).await?;
            }

            ProtocolMessage::Bind(bind) => match prepared_sql.get(bind.statement()) {
                Some(sql) => {
                    bound_sql = sql.clone();
                    stream.send(&BindComplete).await?;
                }
                None => {
                    stream
                        .send(&statement_does_not_exist(bind.statement()))
                        .await?;
                    skip_until_sync = true;
                }
            },

            ProtocolMessage::Describe(describe) => {
                let sql = if describe.is_statement() {
                    stream.send(&ParameterDescription::default()).await?;

                    prepared_sql
                        .get(describe.statement())
                        .map(String::as_str)
                        .unwrap_or_default()
                } else {
                    &bound_sql
                };

                cached_reply(&mut replies, sql)
                    .send_description(&mut stream)
                    .await?;
            }

            ProtocolMessage::Execute(_) => {
                let reply = cached_reply(&mut replies, &bound_sql);
                reply.send_rows(&mut stream).await?;

                in_transaction = reply.starts_transaction.unwrap_or(in_transaction);
            }

            ProtocolMessage::Sync(_) => {
                skip_until_sync = false;
                stream
                    .send_flush(&ReadyForQuery::in_transaction(in_transaction))
                    .await?;
            }

            ProtocolMessage::Close(_) => {
                stream.send(&CloseComplete).await?;
            }

            ProtocolMessage::Other(message) if message.code() == TERMINATE => return Ok(()),

            _ => stream.flush().await?,
        }
    }
}

fn cached_reply<'a>(replies: &'a mut HashMap<String, Reply>, sql: &str) -> &'a Reply {
    if !replies.contains_key(sql) {
        replies.insert(sql.to_owned(), Reply::new(sql));
    }

    &replies[sql]
}

fn statement_does_not_exist(name: &str) -> ErrorResponse {
    ErrorResponse {
        severity: "ERROR".into(),
        code: "26000".into(),
        message: format!(r#"prepared statement "{name}" does not exist"#),
        ..Default::default()
    }
}

async fn handshake(stream: &mut Stream) -> Result<(), Error> {
    while Startup::from_stream(stream).await? == Startup::Ssl {
        stream.write_all(b"N").await?;
        stream.flush().await?;
    }

    stream.send(&Authentication::Ok).await?;
    for parameter in ParameterStatus::fake() {
        stream.send(&parameter).await?;
    }
    stream.send(&BackendKeyData::random_legacy()).await?;
    stream.send_flush(&ReadyForQuery::idle()).await?;

    Ok(())
}

struct Reply {
    row_description: Option<Bytes>,
    rows_and_command_complete: Bytes,
    starts_transaction: Option<bool>,
}

impl Reply {
    fn new(sql: &str) -> Self {
        let parsed = pg_raw_parse::parse(sql).ok();
        let stmt = parsed.as_ref().and_then(|parsed| parsed.stmts().next());

        let (columns, row_count, tag, starts_transaction) = match stmt {
            Some(Node::SelectStmt(select)) => {
                let columns = select_columns(select);
                let row_count = if reads_catalog(select) {
                    0
                } else if returns_one_row(select) {
                    1
                } else {
                    limit_count(select)
                        .unwrap_or(ROWS_PER_SHARD)
                        .min(ROWS_PER_SHARD)
                };

                (
                    Some(columns),
                    row_count,
                    format!("SELECT {row_count}"),
                    None,
                )
            }

            Some(Node::InsertStmt(insert)) => {
                let columns = insert.returning_clause().map(returning_columns);
                (columns, 1, "INSERT 0 1".into(), None)
            }

            Some(Node::UpdateStmt(update)) => {
                let columns = update.returning_clause().map(returning_columns);
                (columns, 1, "UPDATE 1".into(), None)
            }

            Some(Node::DeleteStmt(delete)) => {
                let columns = delete.returning_clause().map(returning_columns);
                (columns, 1, "DELETE 1".into(), None)
            }

            Some(Node::TransactionStmt(transaction)) => {
                use nodes::TransactionStmtKind::*;

                let (tag, starts_transaction) = match transaction.kind {
                    TRANS_STMT_BEGIN | TRANS_STMT_START => ("BEGIN", true),
                    TRANS_STMT_ROLLBACK => ("ROLLBACK", false),
                    _ => ("COMMIT", false),
                };

                (None, 0, tag.into(), Some(starts_transaction))
            }

            _ => {
                let tag = sql
                    .split_whitespace()
                    .next()
                    .unwrap_or("SELECT")
                    .to_uppercase();

                (None, 0, tag, None)
            }
        };

        let mut rows_and_command_complete = BytesMut::new();
        if let Some(columns) = &columns {
            for row in 0..row_count {
                rows_and_command_complete.extend_from_slice(&data_row(columns, row).to_bytes());
            }
        }
        rows_and_command_complete.extend_from_slice(&CommandComplete::new(tag).to_bytes());

        Self {
            row_description: columns.map(|columns| RowDescription::new(&columns).to_bytes()),
            rows_and_command_complete: rows_and_command_complete.freeze(),
            starts_transaction,
        }
    }

    async fn send_description(&self, stream: &mut Stream) -> Result<(), Error> {
        match &self.row_description {
            Some(row_description) => stream.write_all(row_description).await?,
            None => {
                stream.send(&NoData).await?;
            }
        }

        Ok(())
    }

    async fn send_rows(&self, stream: &mut Stream) -> Result<(), Error> {
        stream.write_all(&self.rows_and_command_complete).await?;

        Ok(())
    }
}

fn select_columns(select: &nodes::SelectStmt) -> Vec<Field> {
    let tables: Vec<&str> = select
        .from_clause()
        .into_iter()
        .flat_map(table_names)
        .collect();

    select
        .target_list()
        .into_iter()
        .flat_map(|target| target_columns(target, &tables))
        .collect()
}

fn returning_columns(returning: &nodes::ReturningClause) -> Vec<Field> {
    returning
        .exprs()
        .into_iter()
        .filter_map(|node| match node {
            Node::ResTarget(target) => Some(target),
            _ => None,
        })
        .flat_map(|target| target_columns(target, &[]))
        .collect()
}

fn table_names(node: Node<'_>) -> Vec<&str> {
    match node {
        Node::RangeVar(table) => table.relname().into_iter().collect(),
        Node::JoinExpr(join) => [join.larg(), join.rarg()]
            .into_iter()
            .flat_map(table_names)
            .collect(),
        _ => vec![],
    }
}

fn target_columns(target: &nodes::ResTarget, tables: &[&str]) -> Vec<Field> {
    let value = target.val();

    if is_star(value) {
        return TABLE_COLUMNS
            .iter()
            .filter(|(name, _)| tables.contains(name))
            .flat_map(|(_, columns)| columns.iter())
            .map(|column| Field::bigint(column))
            .collect();
    }

    let name = target.name().unwrap_or(match value {
        Node::ColumnRef(column) => column
            .fields()
            .into_iter()
            .next_back()
            .and_then(Node::as_str)
            .unwrap_or("?column?"),
        Node::FuncCall(func) => function_name(func).unwrap_or("?column?"),
        _ => "?column?",
    });

    let is_avg = matches!(value, Node::FuncCall(func) if function_name(func) == Some("avg"));

    vec![if is_avg {
        Field::numeric(name)
    } else {
        Field::bigint(name)
    }]
}

fn function_name(func: &nodes::FuncCall) -> Option<&str> {
    func.funcname()
        .into_iter()
        .next_back()
        .and_then(Node::as_str)
}

fn is_star(node: Node<'_>) -> bool {
    matches!(node, Node::ColumnRef(column) if matches!(
        column.fields().into_iter().next_back(),
        Some(Node::A_Star(_))
    ))
}

fn reads_catalog(select: &nodes::SelectStmt) -> bool {
    select
        .from_clause()
        .into_iter()
        .flat_map(table_names)
        .any(|table| table.starts_with("pg_") || table.starts_with("information_schema"))
}

fn returns_one_row(select: &nodes::SelectStmt) -> bool {
    select.group_clause().is_empty()
        && select.target_list().into_iter().any(|target| {
            matches!(target.val(), Node::FuncCall(func) if matches!(
                function_name(func),
                Some("count" | "sum" | "avg" | "min" | "max")
            ))
        })
}

fn limit_count(select: &nodes::SelectStmt) -> Option<usize> {
    match select.limit_count() {
        Node::A_Const(limit) => match limit.val() {
            Some(ConstValue::Integer(limit)) => usize::try_from(limit).ok(),
            _ => None,
        },
        _ => None,
    }
}

fn data_row(columns: &[Field], row: usize) -> DataRow {
    let mut data = DataRow::new();
    for _ in columns {
        data.add(row as i64 + 1);
    }

    data
}
