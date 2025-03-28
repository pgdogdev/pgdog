use std::{collections::HashSet, sync::Arc};

use parking_lot::Mutex;

use crate::{
    frontend::{self, prepared_statements::GlobalCache},
    net::messages::{parse::Parse, RowDescription},
};

#[derive(Debug)]
pub struct PreparedStatements {
    cache: Arc<Mutex<GlobalCache>>,
    names: HashSet<String>,
}

impl Default for PreparedStatements {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.cache.lock().row_description(name)
    }

    pub fn describe(&self, name: &str, row_description: &RowDescription) {
        self.cache.lock().describe(name, row_description);
    }

    pub fn remove(&mut self, name: &str) -> bool {
        self.names.remove(name)
    }

    /// Indicate all prepared statements have been removed.
    pub fn clear(&mut self) {
        self.names.clear();
    }
}
