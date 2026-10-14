use itertools::Itertools;

use crate::net::{
    BindComplete, CloseComplete, CommandComplete, DataRow, Field, NoData, NoticeResponse,
    ParameterDescription, ParameterStatus, ParseComplete, Protocol, ProtocolMessage, ReadyForQuery,
    RowDescription, parameter::ParameterValue,
};

use super::*;

/// Ordered responses to commands this engine handles itself.
#[derive(Debug)]
pub(super) struct FakeResponse {
    commands: Vec<FakeCommand>,
}

impl FakeResponse {
    pub(super) fn new(commands: impl IntoIterator<Item = FakeCommand>) -> Self {
        Self {
            commands: commands.into_iter().collect(),
        }
    }

    pub(super) fn command(command: impl Into<String>) -> Self {
        Self::new([FakeCommand::new(command)])
    }

    /// Respond in protocol order, appending at most one ReadyForQuery.
    /// Passing `None` suppresses ReadyForQuery for intermediate simple queries.
    fn messages(
        &self,
        client_messages: &[ProtocolMessage],
        ready: Option<ReadyForQuery>,
    ) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut commands = self.commands.iter().peekable();
        let mut send_ready = false;

        for message in client_messages {
            match message {
                ProtocolMessage::Parse(_) => messages.push(ParseComplete.message()),
                ProtocolMessage::Bind(_) => messages.push(BindComplete.message()),
                ProtocolMessage::Describe(describe) => {
                    if describe.is_statement() {
                        messages.push(ParameterDescription::default().message());
                    }
                    messages.push(
                        commands
                            .peek()
                            .and_then(|command| command.row_description.as_ref())
                            .map(|description| description.message())
                            .unwrap_or_else(|| NoData.message()),
                    );
                }
                ProtocolMessage::Execute(_) => {
                    if let Some(command) = commands.next() {
                        command.append_messages(&mut messages, false);
                    }
                }
                ProtocolMessage::Query(_) => {
                    for command in commands.by_ref() {
                        command.append_messages(&mut messages, true);
                    }
                    send_ready = true;
                }
                ProtocolMessage::Sync(_) => send_ready = true,
                ProtocolMessage::Close(_) => messages.push(CloseComplete.message()),
                _ => (),
            }
        }

        if send_ready && let Some(ready) = ready {
            messages.push(ready.message());
        }

        messages
    }
}

/// Command completion and its optional result and notifications.
#[derive(Debug)]
pub(super) struct FakeCommand {
    command: String,
    row_description: Option<RowDescription>,
    rows: Vec<DataRow>,
    notice: Option<NoticeResponse>,
    parameter_status: Option<ParameterStatus>,
}

impl FakeCommand {
    pub(super) fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            row_description: None,
            rows: Vec::new(),
            notice: None,
            parameter_status: None,
        }
    }

    pub(super) fn with_params<'a>(
        self,
        columns: &[&str],
        values: impl IntoIterator<Item = Option<&'a ParameterValue>>,
    ) -> Self {
        let row_description =
            RowDescription::new(&columns.iter().map(|col| Field::text(col)).collect_vec());

        let mut row = DataRow::new();
        for val in values {
            row.add(val);
        }

        self.with_rows(row_description, [row])
    }

    /// Attach a result set, including its description even when no rows match.
    pub(super) fn with_rows(
        mut self,
        row_description: RowDescription,
        rows: impl IntoIterator<Item = DataRow>,
    ) -> Self {
        self.row_description = Some(row_description);
        self.rows = rows.into_iter().collect();
        self
    }

    pub(super) fn with_notice(mut self, notice: NoticeResponse) -> Self {
        self.notice = Some(notice);
        self
    }

    pub(super) fn with_parameter_status(mut self, status: ParameterStatus) -> Self {
        self.parameter_status = Some(status);
        self
    }

    fn append_messages(&self, messages: &mut Vec<Message>, simple: bool) {
        if let Some(notice) = &self.notice {
            messages.push(notice.message());
        }
        if simple && let Some(description) = &self.row_description {
            messages.push(description.message());
        }
        for row in &self.rows {
            messages.push(row.message());
        }
        messages.push(CommandComplete::new(&self.command).message());
        if let Some(status) = &self.parameter_status {
            messages.push(status.message());
        }
    }
}

impl QueryEngine {
    /// Respond to a command sent by the client
    /// in a way that won't make it suspicious.
    pub(super) async fn fake_command_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        response: &FakeResponse,
    ) -> Result<(), Error> {
        let ready = if context.pipeline.is_simple() && !context.pipeline.is_done() {
            None
        } else {
            Some(ReadyForQuery::in_transaction(context.in_transaction()))
        };
        let messages = response.messages(client_messages, ready);
        let sent = context.stream.send_many(&messages).await?;
        self.stats.sent(sent);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_n;

    use super::*;
    use crate::net::{Bind, Close, Describe, Execute, Parse, Query, Sync};

    #[test]
    fn simple_commands_in_order() {
        let value = ParameterValue::from("second");
        let response = FakeResponse::new([
            FakeCommand::new("SET")
                .with_notice(NoticeResponse::from(
                    ErrorResponse::set_local_outside_transaction(),
                ))
                .with_parameter_status(ParameterStatus::from(("application_name", "first"))),
            FakeCommand::new("SELECT 1").with_params(&["set_config"], [Some(&value)]),
            FakeCommand::new("RESET"),
        ]);

        for ready in [
            None,
            Some(ReadyForQuery::idle()),
            Some(ReadyForQuery::in_transaction(true)),
        ] {
            let messages = response.messages(&[Query::new("SET; SELECT; RESET").into()], ready);
            let mut codes = vec!['N', 'C', 'S', 'T', 'D', 'C', 'C'];
            if let Some(ready) = ready {
                codes.push('Z');
                assert_eq!(
                    ReadyForQuery::try_from(messages.last().expect("ready").clone())
                        .expect("ReadyForQuery")
                        .status,
                    ready.status,
                );
            }
            assert_eq!(
                messages.iter().map(|message| message.code()).collect_vec(),
                codes
            );
            for (index, command) in [(1, "SET"), (5, "SELECT 1"), (6, "RESET")] {
                assert_eq!(
                    CommandComplete::try_from(messages[index].clone())
                        .expect("completion")
                        .command(),
                    command,
                );
            }
            let status = ParameterStatus::try_from(messages[2].clone()).expect("parameter status");
            assert_eq!(status.name, "application_name");
            assert_eq!(status.value, ParameterValue::from("first"));
            assert_eq!(
                DataRow::try_from(messages[4].clone())
                    .expect("data row")
                    .get_text(0),
                Some("second".into()),
            );
        }
    }

    #[test]
    fn extended_commands_in_order() {
        let first = ParameterValue::from("first");
        let second = ParameterValue::from("second");
        let response = FakeResponse::new([
            FakeCommand::new("SELECT 1").with_params(&["first"], [Some(&first)]),
            FakeCommand::new("SELECT 2").with_params(&["second"], [Some(&second)]),
        ]);
        let mut request = vec![
            Parse::named("first", "SELECT 1").into(),
            Bind::new_statement("first").into(),
            Describe::new_statement("first").into(),
            Execute::new().into(),
            Parse::named("second", "SELECT 2").into(),
            Bind::new_statement("second").into(),
            Describe::new_portal("").into(),
            Execute::new().into(),
            Close::named("second").into(),
        ];
        let mut codes = vec!['1', '2', 't', 'T', 'D', 'C', '1', '2', 'T', 'D', 'C', '3'];

        for sync in [false, true] {
            if sync {
                request.push(Sync.into());
                codes.push('Z');
            }
            let messages = response.messages(&request, Some(ReadyForQuery::idle()));
            assert_eq!(
                messages.iter().map(|message| message.code()).collect_vec(),
                codes
            );
            for (index, value) in [(4, "first"), (9, "second")] {
                assert_eq!(
                    DataRow::try_from(messages[index].clone())
                        .expect("data row")
                        .get_text(0),
                    Some(value.into()),
                );
            }
            for (index, command) in [(5, "SELECT 1"), (10, "SELECT 2")] {
                assert_eq!(
                    CommandComplete::try_from(messages[index].clone())
                        .expect("completion")
                        .command(),
                    command,
                );
            }
        }
    }

    #[test]
    fn describe_without_execute() {
        let response = FakeResponse::command("SET");
        let messages = response.messages(
            &[
                Parse::named("set", "SET work_mem = '4MB'").into(),
                Describe::new_statement("set").into(),
                Sync.into(),
            ],
            Some(ReadyForQuery::idle()),
        );
        assert_eq!(
            messages.iter().map(|message| message.code()).collect_vec(),
            ['1', 't', 'n', 'Z']
        );
    }

    #[test]
    fn sync_without_commands() {
        let response = FakeResponse::new([]);
        let messages = response.messages(&[Sync.into()], Some(ReadyForQuery::idle()));
        assert_eq!(
            messages.iter().map(|message| message.code()).collect_vec(),
            ['Z']
        );
    }

    #[test]
    fn multiple_data_rows() {
        for row_count in 0..=3 {
            let rows = (0..row_count).map(|index| {
                let mut row = DataRow::new();
                row.add(index.to_string().as_str());
                row
            });
            let response = FakeResponse::new([
                FakeCommand::new(format!("SELECT {row_count}"))
                    .with_rows(RowDescription::new(&[Field::text("value")]), rows),
                FakeCommand::new("RESET"),
            ]);
            for request in [
                vec![Query::new("SELECT value; RESET work_mem").into()],
                vec![
                    Describe::new_portal("").into(),
                    Execute::new().into(),
                    Execute::new().into(),
                    Sync.into(),
                ],
            ] {
                let messages = response.messages(&request, Some(ReadyForQuery::idle()));
                let mut codes = vec!['T'];
                codes.extend(repeat_n('D', row_count));
                codes.extend(['C', 'C', 'Z']);
                assert_eq!(
                    messages.iter().map(|message| message.code()).collect_vec(),
                    codes
                );
                for (index, message) in messages[1..row_count + 1].iter().enumerate() {
                    assert_eq!(
                        DataRow::try_from(message.clone())
                            .expect("data row")
                            .get_text(0),
                        Some(index.to_string()),
                    );
                }
            }
        }
    }
}
