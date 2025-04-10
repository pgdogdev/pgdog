use super::*;
use crate::frontend::buffer::BufferedQuery;

#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(Box<CopyParser>),
    StartTransaction(BufferedQuery),
    CommitTransaction,
    RollbackTransaction,
    StartReplication,
    ReplicationMeta,
    Set { name: String, value: String },
    PreparedStatement(Prepare),
    Rewrite(String),
}
