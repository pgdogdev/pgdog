use std::ops::Deref;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionType {
    ReadOnly,
    #[default]
    ReadWrite,
    Implicit,
    ErrorReadWrite,
    ErrorReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Transaction {
    pub transaction_type: TransactionType,
    pub source: TransactionSource,
}

impl Transaction {
    pub fn is_automatic(&self) -> bool {
        self.source == TransactionSource::Automatic
    }
}

impl Deref for Transaction {
    type Target = TransactionType;

    fn deref(&self) -> &Self::Target {
        &self.transaction_type
    }
}

impl From<TransactionType> for Transaction {
    fn from(value: TransactionType) -> Self {
        Self {
            transaction_type: value,
            source: TransactionSource::Client,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionSource {
    #[default]
    Client,
    Automatic,
}

impl TransactionType {
    pub fn read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    pub fn write(&self) -> bool {
        !self.read_only()
    }

    pub fn error(&self) -> bool {
        matches!(self, Self::ErrorReadWrite | Self::ErrorReadOnly)
    }
}
