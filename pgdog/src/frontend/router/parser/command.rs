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

impl Command {
    pub(crate) fn dry_run(self) -> Self {
        match self {
            Command::Query(mut query) => {
                query.set_shard(0);
                Command::Query(query)
            }

            Command::Copy(_) => Command::Query(Route::write(Some(0))),
            _ => self,
        }
    }
}
