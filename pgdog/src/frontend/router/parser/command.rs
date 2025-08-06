use super::*;
use crate::{frontend::buffer::BufferedQuery, net::parameter::ParameterValue};
use lazy_static::lazy_static;

#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(Box<CopyParser>),
    StartTransaction(BufferedQuery),
    CommitTransaction,
    RollbackTransaction,
    StartReplication,
    ReplicationMeta,
    Set {
        name: String,
        value: ParameterValue,
    },
    PreparedStatement(Prepare),
    Rewrite(String),
    Shards(usize),
    Deallocate,
    Listen {
        channel: String,
        shard: Shard,
    },
    Notify {
        channel: String,
        payload: String,
        shard: Shard,
    },
    Unlisten(String),
}

impl Command {
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route = Route::write(Shard::All);
        }

        match self {
            Self::Query(route) => route,
            _ => &DEFAULT_ROUTE,
        }
    }
}

impl Default for Command {
    fn default() -> Self {
        Command::Query(Route::write(Shard::All))
    }
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
                query.set_shard_mut(0);
                Command::Query(query)
            }

            Command::Copy(_) => Command::Query(Route::write(Some(0))),
            _ => self,
        }
    }
}
