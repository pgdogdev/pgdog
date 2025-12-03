use std::{ops::Deref, sync::Arc};

use super::Error;
use crate::{
    net::{Bind, Datum},
    unique_id::UniqueId,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct UniqueIdPlan {
    /// Parameter number.
    pub(super) param_ref: i32,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RewritePlan {
    /// How many unique IDs to add to the Bind message.
    pub(super) unique_ids: Vec<UniqueIdPlan>,
}

#[derive(Debug, Clone, Default, PartialEq)]
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::bind::Parameter;
    use std::env::set_var;

    #[test]
    fn test_apply_bind_adds_parameters() {
        unsafe {
            set_var("NODE_ID", "pgdog-test-1");
        }

        // Create a rewrite plan expecting params $3 and $4
        let plan = RewritePlan {
            unique_ids: vec![UniqueIdPlan { param_ref: 3 }, UniqueIdPlan { param_ref: 4 }],
        };

        // Create a Bind with 2 existing parameters
        let mut bind = Bind::new_params(
            "",
            &[
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
            ],
        );

        // Apply the plan
        plan.apply_bind(&mut bind).unwrap();

        // Verify we now have 4 parameters
        assert_eq!(bind.params_raw().len(), 4);

        // Verify the added parameters are BIGINT values (8 bytes in text format)
        let param3 = bind.parameter(2).unwrap().unwrap();
        let param4 = bind.parameter(3).unwrap().unwrap();

        // The parameters should be valid bigints
        let id3 = param3.bigint();
        let id4 = param4.bigint();
        assert!(id3.is_some(), "Third parameter should be a valid bigint");
        assert!(id4.is_some(), "Fourth parameter should be a valid bigint");

        // IDs should be different (unique)
        assert_ne!(id3, id4);
    }

    #[test]
    fn test_apply_bind_parameter_count_mismatch() {
        unsafe {
            set_var("NODE_ID", "pgdog-test-1");
        }

        // Create a plan expecting param $5 (but bind only has 2 params, so next will be $3)
        let plan = RewritePlan {
            unique_ids: vec![UniqueIdPlan { param_ref: 5 }],
        };

        // Create a Bind with 2 existing parameters
        let mut bind = Bind::new_params(
            "",
            &[
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
                Parameter {
                    len: 2,
                    data: "{}".into(),
                },
            ],
        );

        // Apply should fail due to mismatch
        let result = plan.apply_bind(&mut bind);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_bind_empty_plan() {
        // Empty plan should be a no-op
        let plan = RewritePlan::default();

        let mut bind = Bind::new_params(
            "",
            &[Parameter {
                len: 4,
                data: "test".into(),
            }],
        );

        plan.apply_bind(&mut bind).unwrap();

        // Should still have just 1 parameter
        assert_eq!(bind.params_raw().len(), 1);
    }

    #[test]
    fn test_apply_bind_single_unique_id() {
        unsafe {
            set_var("NODE_ID", "pgdog-test-1");
        }

        // Create a plan for a single unique ID as $2
        let plan = RewritePlan {
            unique_ids: vec![UniqueIdPlan { param_ref: 2 }],
        };

        // Create a Bind with 1 existing parameter
        let mut bind = Bind::new_params(
            "",
            &[Parameter {
                len: 4,
                data: "test".into(),
            }],
        );

        plan.apply_bind(&mut bind).unwrap();

        // Should now have 2 parameters
        assert_eq!(bind.params_raw().len(), 2);

        // Second parameter should be a bigint
        let param2 = bind.parameter(1).unwrap().unwrap();
        assert!(param2.bigint().is_some());
    }

    #[test]
    fn test_immutable_plan_apply_bind() {
        unsafe {
            set_var("NODE_ID", "pgdog-test-1");
        }

        // Test that ImmutableRewritePlan can also apply to binds via Deref
        let plan = RewritePlan {
            unique_ids: vec![UniqueIdPlan { param_ref: 1 }],
        }
        .freeze();

        let mut bind = Bind::default();
        plan.apply_bind(&mut bind).unwrap();

        assert_eq!(bind.params_raw().len(), 1);
    }
}
