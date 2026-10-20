use pg_stream::{
    PgConnection, PgMessage, PgProtocol,
    message::{Bindable, TransactionStatus},
    startup::{AuthenticationMode, ConnectionBuilder},
};
use tokio::net::TcpStream;

use crate::integration::subqueries::TestCase;

#[derive(Debug, Clone, Copy)]
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
    mut sql: String,
    protocol: Protocol,
    commands: Option<&TestCase>,
) -> Vec<WireMessage> {
    match protocol {
        Protocol::Simple => {
            if let Some(commands) = commands {
                // If we have a Bind parameter, just replace
                // the parameter with its content, so we can
                // run it in the simple protocol too.
                for (index, param) in commands.bind_params.iter().enumerate() {
                    sql = sql.replace(format!("${}", index + 1).as_str(), param.as_str());
                }
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
        let message = WireMessage::from(conn.recv().await.unwrap());
        let done = matches!(message, WireMessage::ReadyForQuery(_));
        messages.push(message);

        if done {
            return messages;
        }
    }
}
