use std::sync::Arc;

use crate::{frontend::RewritePlan, net::Prepare, stats::memory::MemoryUsage};

use super::prelude::*;

#[derive(Debug, Clone)]
pub struct Statement {
    pub(super) stmt: StatementType,
    pub(super) row_description: Option<RowDescription>,
    pub(super) cache_key: CacheKey,
}

#[derive(Debug, Clone)]
pub(crate) enum StatementType {
    Parse {
        parse: Parse,
        rewrite: Option<Parse>,
    },

    Prepare {
        prepare: Prepare,
        rewrite_plan: Arc<RewritePlan>,
    },
}

impl MemoryUsage for StatementType {
    fn memory_usage(&self) -> usize {
        match self {
            Self::Prepare { prepare, .. } => prepare.len(),
            Self::Parse { parse, rewrite } => {
                parse.len()
                    + rewrite
                        .as_ref()
                        .map(|rewrite| rewrite.len())
                        .unwrap_or_default()
            }
        }
    }
}

impl MemoryUsage for Statement {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.stmt.memory_usage()
            + if let Some(row_description) = &self.row_description {
                row_description.memory_usage()
            } else {
                0
            }
            + self.cache_key.memory_usage()
    }
}

impl Statement {
    pub(crate) fn parse(&self) -> Option<Parse> {
        match self.stmt {
            StatementType::Parse { ref parse, .. } => Some(parse.clone()),
            _ => None,
        }
    }

    pub(super) fn prepare_and_rewrite(&self) -> Option<(Prepare, Arc<RewritePlan>)> {
        match self.stmt {
            StatementType::Prepare {
                ref prepare,
                ref rewrite_plan,
            } => Some((prepare.clone(), rewrite_plan.clone())),
            _ => None,
        }
    }

    pub(crate) fn rewritten_parse(&self) -> Option<Parse> {
        match self.stmt {
            StatementType::Parse { ref rewrite, .. } => rewrite.clone(),
            _ => None,
        }
    }

    pub(super) fn query(&self) -> &str {
        match self.stmt {
            StatementType::Parse { ref parse, .. } => parse.query(),
            StatementType::Prepare { ref prepare, .. } => prepare.query(),
        }
    }

    pub(super) fn cache_key(&self) -> &CacheKey {
        &self.cache_key
    }

    pub(super) fn set_rewrite(&mut self, parse: &Parse) {
        if let StatementType::Parse {
            ref mut rewrite, ..
        } = self.stmt
        {
            *rewrite = Some(parse.clone())
        }
    }
}
