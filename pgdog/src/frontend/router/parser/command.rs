use super::*;
use crate::{frontend::buffer::BufferedQuery, net::parameter::ParameterValue};

#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(Box<CopyParser>),
    StartTransaction(BufferedQuery),
    CommitTransaction,
    RollbackTransaction,
    StartReplication,
    ReplicationMeta,
    Set { name: String, value: ParameterValue },
    PreparedStatement(Prepare),
    Rewrite(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetVal {
    Integer(i64),
    Boolean(bool),
    String(String),
}

impl From<String> for SetVal {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i32> for SetVal {
    fn from(value: i32) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<bool> for SetVal {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl std::fmt::Display for SetVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetVal::String(s) => write!(f, "{}", s),
            SetVal::Integer(i) => write!(f, "{}", i),
            SetVal::Boolean(b) => write!(f, "{}", b),
        }
    }
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
