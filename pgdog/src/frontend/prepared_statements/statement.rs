use crate::{
    frontend::router::parser::rewrite::statement::{offset::OffsetPlan, plan::GeneratedParam},
    net::Prepare,
    stats::memory::MemoryUsage,
};

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub(super) stmt: StatementType,
    pub(super) row_description: Option<RowDescription>,
    pub(super) cache_key: CacheKey,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedPlan {
    pub(crate) prepare: Prepare,

    /// The number of calls to `pgdog.unique_id` which were previously
    /// rewritten. If this value is greater than zero, it is expected
    /// that the query in the [`Parse`] message referenced by
    /// [`Self::prepare`] was previously rewritten to replace those calls
    /// with bind parameter placeholder numbered after all others
    pub(crate) unique_ids: u16,

    /// Used to keep track of LIMIT + OFFSET queries (stemming from Prepare),
    /// where we have to re-write `A_Const` nodes with `ParamRefs`, so that we can dynamically
    /// modify limit/offset values before execution if it ends up being cross-shard.
    pub(crate) offset_plan: Option<OffsetPlan>,

    pub(crate) generated_params: Vec<GeneratedParam>,
}

#[derive(Debug, Clone)]
pub(crate) enum StatementType {
    Parse {
        parse: Parse,
        rewrite: Option<Parse>,
        client_params: Option<u16>,
    },

    Prepare(PreparedPlan),
}

impl MemoryUsage for StatementType {
    fn memory_usage(&self) -> usize {
        match self {
            Self::Prepare(plan) => plan.prepare.len(),
            Self::Parse { parse, rewrite, .. } => {
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

    pub(super) fn prepared_plan(&self) -> Option<PreparedPlan> {
        match &self.stmt {
            StatementType::Prepare(plan) => Some(plan.clone()),
            _ => None,
        }
    }

    pub(crate) fn rewritten_parse(&self) -> Option<Parse> {
        match self.stmt {
            StatementType::Parse { ref rewrite, .. } => rewrite.clone(),
            _ => None,
        }
    }

    pub(super) fn cache_key(&self) -> &CacheKey {
        &self.cache_key
    }

    pub(crate) fn client_params(&self) -> Option<u16> {
        match self.stmt {
            StatementType::Parse { client_params, .. } => client_params,
            _ => None,
        }
    }

    pub(super) fn set_rewrite(&mut self, parse: &Parse, params: u16) {
        if let StatementType::Parse {
            ref mut rewrite,
            ref mut client_params,
            ..
        } = self.stmt
        {
            *rewrite = Some(parse.clone());
            *client_params = Some(params);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Statement, StatementType};

    impl Statement {
        pub(crate) fn query(&self) -> &str {
            match self.stmt {
                StatementType::Parse { ref parse, .. } => parse.query(),
                StatementType::Prepare(ref plan) => plan.prepare.query(),
            }
        }
    }
}
