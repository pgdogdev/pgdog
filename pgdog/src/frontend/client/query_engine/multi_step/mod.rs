pub(crate) mod error;
pub mod forward_check;
pub mod insert;
pub mod state;
pub mod update;

pub(crate) use error::{Error, UpdateError};
pub(crate) use forward_check::*;
pub(crate) use insert::InsertMulti;
pub use state::{CommandType, MultiServerState};
pub(crate) use update::UpdateMulti;

#[cfg(test)]
mod test;
