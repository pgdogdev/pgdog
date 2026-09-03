use super::Table;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

#[derive(Debug, Clone, Default)]
pub(crate) struct Tables {
    /// Omnisharded tables.
    /// (schema, name) -> Table definition
    pub(crate) omnisharded: HashMap<(String, String), Table>,
    /// Shard -> Tables
    pub(crate) tables: HashMap<usize, Vec<Table>>,
}

impl Deref for Tables {
    type Target = HashMap<usize, Vec<Table>>;

    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

impl DerefMut for Tables {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tables
    }
}
