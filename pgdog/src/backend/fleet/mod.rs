//! Cross-instance coordination for a fleet of pgdog processes sharing
//! the same databases: who is alive (registry), a coordinator/follower
//! protocol over a designated "medium" database, and the local write
//! barrier a coordination arms.

pub mod barrier;
pub mod registry;

pub(crate) mod coordinator;
pub(crate) mod follower;
pub(crate) mod protocol;

pub(crate) use coordinator::{Coordinator, Discovery};
pub(crate) use follower::Follower;
pub(crate) use protocol::Topic;
