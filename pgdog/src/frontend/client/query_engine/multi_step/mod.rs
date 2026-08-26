pub(crate) mod error;
pub(crate) mod forward_check;
pub(crate) mod insert;
pub(crate) mod state;
pub(crate) mod update;

pub(crate) use error::{Error, UpdateError};
pub(crate) use forward_check::*;
pub(crate) use insert::InsertMulti;
pub(crate) use state::{CommandType, MultiServerState};
pub(crate) use update::UpdateMulti;

#[cfg(test)]
mod test;
