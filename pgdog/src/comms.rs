//! Shared communications between clients.

use std::collections::HashMap;

use tokio::sync::{mpsc, watch};

use crate::net::messages::BackendKeyData;

pub struct Comms {
    stats: HashMap<BackendKeyData, watch::Receiver<()>>,
}
