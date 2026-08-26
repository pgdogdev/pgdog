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
