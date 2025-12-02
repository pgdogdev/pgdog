//! Rewrite engine state. To be preserved between requests.

use std::collections::HashMap;

use crate::frontend::router::parser::cache::CachedAst;

#[derive(Debug, Default, Clone)]
pub struct RewriteState {
    originals: HashMap<String, CachedAst>,
}

impl RewriteState {
    /// Save original AST into rewrite state.
    ///
    /// We use it to rewrite Bind messages. Instead of encapsulating
    /// complex rewrite rules in an enum, we walk the AST and
    /// perform whatever changes we need.
    ///
    pub fn save(&mut self, name: &str, ast: &CachedAst) {
        self.originals.insert(name.to_string(), ast.clone());
    }

    /// Get the original AST by statement name.
    pub fn get(&self, name: &str) -> Option<&CachedAst> {
        self.originals.get(name)
    }

    /// Remove AST from state.
    pub fn remove(&mut self, name: &str) -> bool {
        self.originals.remove(name).is_some()
    }

    /// Number of ASTs in the state.
    pub fn len(&self) -> usize {
        self.originals.len()
    }
}
