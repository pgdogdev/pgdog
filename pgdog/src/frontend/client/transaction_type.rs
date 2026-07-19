#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionType {
    ReadOnly,
    #[default]
    ReadWrite,
    Implicit,
    ErrorReadWrite,
    ErrorReadOnly,
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
