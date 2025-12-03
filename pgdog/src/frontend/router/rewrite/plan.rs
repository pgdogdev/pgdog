use std::{ops::Deref, sync::Arc};

use super::Error;
use crate::{
    net::{Bind, Datum},
    unique_id::UniqueId,
};

#[derive(Debug, Clone, Default)]
pub struct UniqueIdPlan {
    /// Parameter number.
    pub(super) param_ref: i32,
}

#[derive(Debug, Clone, Default)]
pub struct RewritePlan {
    /// How many unique IDs to add to the Bind message.
    pub(super) unique_ids: Vec<UniqueIdPlan>,
}

#[derive(Debug, Clone, Default)]
pub struct ImmutableRewritePlan {
    /// Compiled rewrite plan, that cannot be modified further.
    pub(super) plan: Arc<RewritePlan>,
}

impl Deref for ImmutableRewritePlan {
    type Target = RewritePlan;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl RewritePlan {
    /// Apply rewrite plan to Bind message.
    ///
    /// N.B. this isn't idempotent, run this only once.
    ///
    pub fn apply_bind(&self, bind: &mut Bind) -> Result<(), Error> {
        for unique_id in &self.unique_ids {
            let id = UniqueId::generator()?.next_id();
            let counter = bind.add_parameter(Datum::Bigint(id))?;
            // Params should be added to plan in order.
            // This validates it.
            if counter != unique_id.param_ref {
                return Err(Error::ParameterCountMismatch);
            }
        }

        Ok(())
    }

    /// Freeze rewrite plan, without any more modifications allowed.
    pub fn freeze(self) -> ImmutableRewritePlan {
        ImmutableRewritePlan {
            plan: Arc::new(self),
        }
    }
}
