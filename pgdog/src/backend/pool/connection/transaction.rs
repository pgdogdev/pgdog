use crate::frontend::BufferedQuery;

#[derive(Debug)]
pub(crate) struct TransactionBinding {
    // Preserves read/write intent of the transaction statement
    // before connecting to the shard(s).
    pub(super) is_read: Option<bool>,
    // Avoid cloning when we transform.
    pub(super) transaction_stmt: Option<BufferedQuery>,
}
