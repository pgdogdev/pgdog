use anyhow::anyhow;
use pg_stream::{
    PgConnection, PgMessage, PgProtocol,
    message::{Bindable, TransactionStatus},
    startup::{AuthenticationMode, ConnectionBuilder},
};
use sqlx::{
    Row,
    postgres::{PgPoolOptions, PgRow},
};
use tokio::net::TcpStream;

use crate::integration::subqueries::{TestCase, test_case::StatementType};

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum Protocol {
    Simple,
    Extended,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Field {
    name: String,
    type_oid: u32,
    type_size: i16,
    type_modifier: i32,
}

/// Backend message to be compared with only parts we expect to match between Postgres and PgDog.
#[derive(Debug, PartialEq)]
pub(crate) enum WireMessage {
    RowDescription(Vec<Field>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    ParameterDescription(Vec<u32>),
    Error {
        severity: String,
        code: String,
        message: String,
    },
    ReadyForQuery(TransactionStatus),
    Other(String),
}

impl From<PgMessage> for WireMessage {
    fn from(message: PgMessage) -> Self {
        match message {
            PgMessage::RowDescription(desc) => Self::RowDescription(
                (0..desc.column_count() as usize)
                    .map(|i| Field {
                        name: desc.column_name(i).unwrap_or_default().into_owned(),
                        type_oid: desc.type_oid(i).unwrap_or_default(),
                        type_size: desc.type_size(i).unwrap_or_default(),
                        type_modifier: desc.type_modifier(i).unwrap_or_default(),
                    })
                    .collect(),
            ),
            PgMessage::DataRow(row) => Self::DataRow(
                (0..row.column_count() as usize)
                    .map(|i| {
                        row.column(i)
                            .map(|value| String::from_utf8_lossy(value).into_owned())
                    })
                    .collect(),
            ),
            PgMessage::CommandComplete(cmd) => Self::CommandComplete(cmd.tag().into_owned()),
            PgMessage::ParameterDescription(desc) => Self::ParameterDescription(
                (0..desc.param_count() as usize)
                    .filter_map(|i| desc.param_oid(i))
                    .collect(),
            ),
            PgMessage::ErrorResponse(err) => Self::Error {
                severity: err.severity().into_owned(),
                code: err.code().into_owned(),
                message: err.message().into_owned(),
            },
            PgMessage::ReadyForQuery(rfq) => Self::ReadyForQuery(rfq.status()),
            other => Self::Other(format!("{other:?}")),
        }
    }
}

pub(crate) async fn connect(port: u16, database: &str) -> PgConnection<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (conn, _) = ConnectionBuilder::new("pgdog")
        .database(database)
        .auth(AuthenticationMode::Password("pgdog".into()))
        .connect(stream)
        .await
        .unwrap();

    conn
}

/// Run the statement and collect every backend message
/// up to *AND INCLUDING* `ReadyForQuery`.
pub(crate) async fn run(
    conn: &mut PgConnection<TcpStream>,
    sql: String,
    protocol: Protocol,
    commands: Option<&TestCase>,
    direct_postgres: bool,
) -> Result<Vec<WireMessage>, anyhow::Error> {
    let app_name_tested =
        commands.map(|cmd| format!("{:?}_{}", protocol, cmd.identifier).to_lowercase());

    // Set the applciation name to a unique identifier to the test case,
    // so that we can figure out what queries belong to this test case
    // when we look at pg_stat_monitor filtering by the application_name
    // to identify what subqueries were executed on what Shard.
    if let Some(app_name_tested) = app_name_tested.as_ref() {
        let set_app_name = format!("SET application_name TO {app_name_tested}");

        conn.query(set_app_name.as_str());
        conn.flush().await.unwrap();

        loop {
            let message = WireMessage::from(conn.recv().await?);
            let done = matches!(message, WireMessage::ReadyForQuery(_));

            if done {
                break;
            }
        }
    }

    match protocol {
        Protocol::Simple => {
            if let Some(commands) = commands.as_ref()
                && !commands.bind_params.is_empty()
            {
                unreachable!("We shouldn't run with bind params in simple protocol");
            }

            conn.query(sql.as_str());
        }

        Protocol::Extended => {
            let params_list: Vec<&dyn Bindable> = commands
                .map(|command| {
                    command
                        .bind_params
                        .iter()
                        .map(|param| param as &dyn Bindable)
                        .collect()
                })
                .unwrap_or(vec![]);

            conn.parse(None)
                .query(sql.as_str())
                .finish()
                .bind(None)
                .finish(&params_list)
                .describe_portal(None)
                .execute(None, 0)
                .sync();
        }
    }

    conn.flush().await.unwrap();

    let mut messages = vec![];
    loop {
        let message = WireMessage::from(conn.recv().await?);
        let done = matches!(message, WireMessage::ReadyForQuery(_));
        messages.push(message);

        if done {
            if !direct_postgres && let Some(app_name_tested) = app_name_tested.as_ref() {
                // Only assert routing if this is going to PgDog. We can't do the same for Postgres (as it's not sharded).
                fetch_pg_stat_statement(commands.unwrap(), app_name_tested).await?;
            }

            return Ok(messages);
        }
    }
}

/// Query `pg_stat_monitor` from each of the Postgres shards to see what queries
/// actually ran when running the Subquery on PgDog.
///
/// This could be multiple statements as we execute subqueries separately when parse,
/// evaluate separately, and inject into the top-level statement.
///
/// So, this allows us to see what queries actually ran (what was detected),
/// as well as where they were routed.
///
/// `TestCase` holds a field pulled from the config for expected routing
/// for each query, as well as the total amount of queries, and a general
/// order.
///
/// TODO(jkaczman): we should be more specific instead of 'select' / 'insert' / etc statements.
async fn fetch_pg_stat_statement(
    test_case: &TestCase,
    app_name_tested: &str,
) -> Result<(), anyhow::Error> {
    let mut actual: Vec<Vec<StatementType>> = Vec::new();
    for shard in [0, 1] {
        let sharded_conn = PgPoolOptions::new()
            .max_connections(1)
            .connect(
                format!(
                    "postgres://pgdog:pgdog@127.0.0.1:5432/shard_{shard}?application_name=sqlx"
                )
                .as_str(),
            )
            .await
            .unwrap();

        // PERF(jkaczman): Technically, this could be a singular query send that doesn't filter by datname,
        // then we break it apart ourselves.
        let statements = sqlx::raw_sql(
            format!("SELECT application_name, query FROM pg_stat_monitor WHERE datname = 'shard_{shard}' ORDER BY stats_since ASC").as_str(),
        )
        .fetch_all(&sharded_conn)
        .await
        .unwrap();

        let relevant_statements: Vec<&PgRow> = statements
            .iter()
            .filter(|statement| {
                statement
                    .get::<&str, &str>("application_name")
                    .eq(app_name_tested)
            })
            .filter(|statement| !statement.get::<&str, &str>("query").starts_with("SET"))
            .collect();

        let statement_types: Vec<StatementType> = relevant_statements
            .iter()
            .map(|row| {
                let query = row.get::<&str, &str>("query");

                StatementType::parse(query)
            })
            .collect();

        actual.push(statement_types);
    }

    // Directly match if there's no 'any' checks
    if test_case.routing_any.is_empty() && actual != test_case.routing_expectations {
        return Err(anyhow!(
            "Doesn't match; actual {actual:?} vs expected: {:?}",
            test_case.routing_expectations
        ));
    }

    let candidates: Vec<Vec<Vec<StatementType>>> = (0..actual.len())
        .map(|any_shard| {
            let mut expected = test_case.routing_expectations.clone();
            expected[any_shard].splice(0..0, test_case.routing_any.iter().cloned());
            expected
        })
        .collect();

    assert!(
        candidates.contains(&actual),
        "routing mismatch: got {actual:?}, expected one of {candidates:?}"
    );

    Ok(())
}
