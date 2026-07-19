//! SHOW LISTENERS.

use crate::backend::pub_sub::listener;

use super::prelude::*;

pub struct ShowListeners;

#[async_trait]
impl Command for ShowListeners {
    fn name(&self) -> String {
        "SHOW LISTENERS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut channels: Vec<_> = listener::stats().into_iter().collect();
        channels.sort_by(|a, b| a.0.cmp(&b.0));

        let mut messages = vec![
            RowDescription::new(&[
                Field::text("channel"),
                Field::numeric("listeners"),
                Field::numeric("received"),
                Field::numeric("dropped"),
            ])
            .message()?,
        ];

        for (channel, stats) in channels {
            let mut data_row = DataRow::new();
            data_row
                .add(channel.as_str())
                .add(stats.listeners as i64)
                .add(stats.recv as i64)
                .add(stats.dropped as i64);
            messages.push(data_row.message()?);
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use crate::net::{FromBytes, RowDescription};

    use super::*;

    #[tokio::test]
    async fn show_listeners_reports_columns() {
        let messages = ShowListeners
            .execute()
            .await
            .expect("show listeners should execute");

        assert_eq!(messages[0].code(), 'T');

        let row_description =
            RowDescription::from_bytes(messages[0].payload()).expect("row description parses");
        let columns: Vec<&str> = row_description
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();

        assert_eq!(columns, ["channel", "listeners", "received", "dropped"]);
    }
}
