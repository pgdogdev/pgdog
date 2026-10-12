#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash, Default)]
pub(crate) enum StatementType {
    SessionControl,     // SET
    TransactionControl, // BEGIN, COMMIT
    Ddl,                // CREATE, DROP, etc.
    #[default]
    Dml,  // INSERT, UPDATE, ..
}
