#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub(crate) enum StatementType {
    SessionControl,     // SET
    TransactionControl, // BEGIN, COMMIT
    Ddl,                // CREATE, DROP, etc.
    Dml,                // INSERT, UPDATE, ..
}
