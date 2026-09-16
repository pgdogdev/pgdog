use chrono::{DateTime, Utc};

/// TODO: Not sure if this Transaction refactor is the best way to store the Transaction start time
///       However, if that field just goes separately on the Client,
///       there's more verbosity everywhere we update the Transaction (and potential for bugs)
#[derive(Debug, Clone, Copy)]
pub(crate) struct Transaction {
    transaction_type: TransactionType,
    start_time: DateTime<Utc>,
}

impl Transaction {
    pub(crate) fn new(transaction_type: TransactionType) -> Self {
        Self {
            transaction_type,
            start_time: Utc::now(),
        }
    }

    pub(crate) fn transaction_type(&self) -> TransactionType {
        self.transaction_type
    }

    pub(crate) fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    //    pub(crate) fn read_only(&self) -> bool {
    //      self.transaction_type.read_only()
    //   }

    pub(crate) fn write(&self) -> bool {
        self.transaction_type.write()
    }

    pub(crate) fn error(&self) -> bool {
        self.transaction_type.error()
    }
}

/// Reference times used to rewrite time functions
/// (e.g. now(), statement_timestamp()) consistently across shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryTimestamps {
    /// Start of the transaction.
    /// If not in transaction, this is same as statement start.
    pub(crate) transaction_start: DateTime<Utc>,
    /// When we received the first message of the client's request.
    pub(crate) statement_start: DateTime<Utc>,
}

impl Default for QueryTimestamps {
    fn default() -> Self {
        QueryTimestamps::now()
    }
}

impl QueryTimestamps {
    pub(crate) fn new(transaction: Option<&Transaction>, statement_start: DateTime<Utc>) -> Self {
        Self {
            transaction_start: transaction
                .map(|t| t.start_time())
                .unwrap_or(statement_start),
            statement_start,
        }
    }

    pub(crate) fn now() -> Self {
        Self::new(None, Utc::now())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum TransactionType {
    ReadOnly,
    #[default]
    ReadWrite,
    Implicit,
    ErrorReadWrite,
    ErrorReadOnly,
}

impl TransactionType {
    pub(crate) fn read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    pub(crate) fn write(&self) -> bool {
        !self.read_only()
    }

    pub(crate) fn error(&self) -> bool {
        matches!(self, Self::ErrorReadWrite | Self::ErrorReadOnly)
    }
}
