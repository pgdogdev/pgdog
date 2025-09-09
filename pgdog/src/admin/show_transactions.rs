//! SHOW TRANSACTIONS (two-phase commit).
use crate::frontend::client::query_engine::two_pc::Manager;

use super::prelude::*;

pub struct ShowTransactions;

#[async_trait]
impl Command for ShowTransactions {
    fn name(&self) -> String {
        "SHOW TRANSACTIONS".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let fields = vec![
            Field::text("database"),
            Field::text("user"),
            Field::text("transaction_name"),
            Field::text("transaction_state"),
        ];

        let mut messages = vec![RowDescription::new(&fields).message()?];

        let manager = Manager::get();
        let transactions = manager.transactions();

        for (transaction, info) in transactions {
            let mut dr = DataRow::new();

            dr.add(&info.identifier.database)
                .add(&info.identifier.user)
                .add(&transaction.to_string())
                .add(&info.phase.to_string());

            messages.push(dr.message()?);
        }

        Ok(messages)
    }
}
