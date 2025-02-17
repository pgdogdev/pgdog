//! Discovery of other PgDog nodes.
//!
//! We're using multicast and broadcasting a packet
//! with a unique identifier to everyone who's listening.
//!
//! This is not particularly reliable since packets can
//! be dropped, multicast can be disabled, and many other reasons
//! I don't know about.
//!
//! Realistically, we should have a preconfigured instance
//! of PgDog that other instances connect to register. IPs in
//! most networks are assigned with DHCP so having a static config
//! for all nodes isn't ideal.

pub mod error;
pub mod listener;
pub mod message;

pub use error::Error;
pub use listener::Listener;
pub use message::{Message, Payload};
