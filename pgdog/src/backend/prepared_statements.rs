use std::{collections::HashSet, sync::Arc};

use parking_lot::Mutex;

use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::messages::parse::Parse,
};

#[derive(Debug)]
pub struct PreparedStatements {
    cache: Arc<Mutex<GlobalCache>>,
    names: HashSet<String>,
}

impl PreparedStatements {
    /// New server prepared statements.
    pub fn new() -> Self {
        Self {
            cache: frontend::PreparedStatements::global(),
            names: HashSet::new(),
        }
    }

    /// The server has prepared this statement already.
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    /// Indicate this statement is prepared on the connection.
    pub fn prepared(&mut self, name: &str) {
        self.names.insert(name.to_owned());
    }

    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.cache.lock().parse(name)
    }
}
