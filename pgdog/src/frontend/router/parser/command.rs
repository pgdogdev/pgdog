use super::*;
use crate::{
    frontend::{BufferedQuery, client::TransactionType},
    net::{Query, parameter::ParameterValue},
};
use lazy_static::lazy_static;

#[derive(Debug, Clone, PartialEq)]
pub struct SetParam {
    pub name: String,
    pub value: Option<ParameterValue>,
    pub local: bool,
}

#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(Box<CopyParser>),
    StartTransaction {
        query: BufferedQuery,
        transaction_type: TransactionType,
        extended: bool,
        route: Route,
    },
    CommitTransaction {
        extended: bool,
    },
    RollbackTransaction {
        extended: bool,
    },
    Set {
        params: Vec<SetParam>,
        route: Route,
        behave_like_select: bool,
    },
    ResetAll,
    InternalField {
        name: String,
        value: String,
    },
    Deallocate,
    Discard {
        extended: bool,
    },
    Listen {
        channel: String,
        shard: Shard,
    },
    Notify {
        channel: String,
        payload: String,
        shard: Shard,
    },
    SimpleQuerySplit {
        queries: Vec<Query>,
    },
    Unlisten(String),
    UniqueId,
}

impl Command {
    pub fn route(&self) -> &Route {
        lazy_static! {
            static ref DEFAULT_ROUTE: Route =
                Route::write(ShardWithPriority::new_default_unset(Shard::All));
        }

        match self {
            Self::Query(route) => route,
            Self::Set { route, .. } => route,
            Self::StartTransaction { route, .. } => route,
            _ => &DEFAULT_ROUTE,
        }
    }
}

impl Default for Command {
    fn default() -> Self {
        Command::Query(Route::write(ShardWithPriority::new_default_unset(
            Shard::All,
        )))
    }
}

impl Command {
    pub(crate) fn dry_run(self) -> Self {
        match self {
            Command::Query(mut query) => {
                query.set_shard(ShardWithPriority::new_override_dry_run(Shard::Direct(0)));
                Command::Query(query)
            }

            Command::Copy(_) => Command::Query(Route::write(
                ShardWithPriority::new_override_dry_run(Shard::Direct(0)),
            )),
            _ => self,
        }
    }
}
