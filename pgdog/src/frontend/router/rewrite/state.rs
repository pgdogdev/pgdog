//! Rewrite engine state. To be preserved between requests.

use std::collections::HashMap;

use bytes::Bytes;

use super::{Error, ImmutableRewritePlan};
use crate::net::{Bind, Parse};

#[derive(Debug, Default, Clone)]
pub struct RewriteState {
    plans: HashMap<Bytes, ImmutableRewritePlan>,
    active_plan: Option<ImmutableRewritePlan>,
}

impl RewriteState {
    /// Save rewrite plan for later use and active it for
    /// this request.
    pub fn save_plan(&mut self, parse: Option<&Parse>, plan: ImmutableRewritePlan) {
        if let Some(parse) = parse {
            self.plans.insert(parse.name_ref(), plan.clone());
        }

        self.active_plan = Some(plan);
    }

    /// Activate plan for Bind, or error out if plan doesn't exist.
    pub fn activate_plan(&mut self, bind: &Bind) -> Result<&ImmutableRewritePlan, Error> {
        if let Some(plan) = self.plans.get(bind.statement_ref()) {
            self.active_plan = Some(plan.clone());
            self.plan()
        } else {
            Err(Error::NoRewrite)
        }
    }

    /// Get currently active rewrite plan.
    pub fn plan(&self) -> Result<&ImmutableRewritePlan, Error> {
        self.active_plan.as_ref().ok_or(Error::NoActiveRewritePlan)
    }
}
