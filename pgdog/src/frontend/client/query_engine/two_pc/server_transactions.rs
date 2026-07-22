use std::ops::Deref;

use crate::{
    backend::{Error, Server},
    net::DataRow,
};

use super::{TwoPcTransaction, TwoPcTransactionOnShard};

#[derive(Debug, Clone)]
pub enum TwoPcServerTransaction {
    Ours {
        txn: TwoPcTransaction,
        user: String,
        database: String,
    },
    Other {
        name: String,
        user: String,
        database: String,
    },
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
        let records: Vec<DataRow> = server
            .fetch_all(
                "SELECT gid, owner, database FROM pg_prepared_xacts WHERE owner = current_user",
            )
            .await?;
        let mut transactions = vec![];

        for record in records {
            let gid = record.get_text(0);
            let user = record.get_text(1).unwrap_or_default();
            let database = record.get_text(2).unwrap_or_default();

            if let Some(gid) = gid {
                let txn = if let Ok(txn) = gid.parse::<TwoPcTransactionOnShard>() {
                    TwoPcServerTransaction::Ours {
                        txn: txn.transaction(),
                        user,
                        database,
                    }
                } else {
                    TwoPcServerTransaction::Other {
                        name: gid,
                        user,
                        database,
                    }
                };

                transactions.push(txn);
            }
        }

        Ok(Self { transactions })
    }
}
