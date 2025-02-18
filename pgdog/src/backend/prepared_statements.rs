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

    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.cache.lock().parse(name)
    }
}
