use std::collections::HashMap;

use crate::frontend::router::rewrite::{RewriteExecutionKind, RewriteExecutionPlan};

use super::{Error, QueryEngine, QueryEngineContext};

type RewriteHandler =
    fn(&mut QueryEngine, &mut QueryEngineContext<'_>, &RewriteExecutionPlan) -> Result<(), Error>;

#[derive(Debug, Default)]
pub struct RewriteDriver {
    handlers: HashMap<RewriteExecutionKind, RewriteHandler>,
}

impl RewriteDriver {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, kind: RewriteExecutionKind, handler: RewriteHandler) {
        self.handlers.insert(kind, handler);
    }

    pub fn handler(&self, kind: &RewriteExecutionKind) -> Option<RewriteHandler> {
        self.handlers.get(kind).copied()
    }
}
