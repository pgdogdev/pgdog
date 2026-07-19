use std::{ops::Deref, str::FromStr};

use crate::{
    backend::{Error, Server},
    net::DataRow,
};

use super::TwoPcTransaction;

#[derive(Debug, Clone)]
pub enum TwoPcServerTransaction {
    Ours(TwoPcTransaction),
    Other { name: String },
}

pub(crate) struct TwoPcTransactions {
    transactions: Vec<TwoPcServerTransaction>,
}

impl Deref for TwoPcTransactions {
    type Target = Vec<TwoPcServerTransaction>;

    fn deref(&self) -> &Self::Target {
        &self.transactions
    }
}

impl TwoPcTransactions {
    /// Load two phase transactions from the server.
    pub(crate) async fn load(server: &mut Server) -> Result<Self, Error> {
        let records: Vec<DataRow> = server.fetch_all("SELECT * FROM pg_prepared_xacts").await?;
        let mut transactions = vec![];

        for record in records {
            let transaction = record.get_text(1).map(|name| {
                if let Ok(ours) = TwoPcTransaction::from_str(&name) {
                    TwoPcServerTransaction::Ours(ours)
                } else {
                    TwoPcServerTransaction::Other { name }
                }
            });

            if let Some(transaction) = transaction {
                transactions.push(transaction);
            }
        }

        Ok(Self { transactions })
    }
}
