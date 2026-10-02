use crate::frontend::BufferedQuery;

/// Transaction state binding. Keeps track of what the client
/// wants while we wait for a real query.
#[derive(Debug)]
pub(crate) struct TransactionBinding {
    // Preserves read/write intent of the transaction statement
    // before connecting to the shard(s).
    //
    // `read_write_split` setting decides if its the transaction statement that makes this
    // call or the first statement inside the transaction does.
    pub(super) is_read: Option<bool>,
    // Avoid cloning when we transform.
    pub(super) transaction_stmt: Option<BufferedQuery>,
}
