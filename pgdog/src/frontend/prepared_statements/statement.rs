use crate::{net::Prepare, stats::memory::MemoryUsage};

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
        /// The number of calls to `pgdog.unique_id` which were previously
        /// rewritten. If this value is greater than zero, it is expected
        /// that the query in the [`Parse`] message referenced by
        /// [`Self::prepare`] was previously rewritten to replace those calls
        /// with bind parameter placeholder numbered after all others
        unique_ids: u16,
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

    pub(super) fn prepare_and_unique_ids(&self) -> Option<(Prepare, u16)> {
        match &self.stmt {
            StatementType::Prepare {
                prepare,
                unique_ids,
            } => Some((prepare.clone(), *unique_ids)),
            _ => None,
        }
    }

    pub(crate) fn rewritten_parse(&self) -> Option<Parse> {
        match self.stmt {
            StatementType::Parse { ref rewrite, .. } => rewrite.clone(),
            _ => None,
        }
    }

    #[cfg(test)]
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
