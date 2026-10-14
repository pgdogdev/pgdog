//! Sticky settings for clients that override
//! default routing behavior determined by the query parser.

use pgdog_config::SessionRole;
use rand::{Rng, rng};

use crate::net::Parameters;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sticky {
    /// Which shard to use for omnisharded queries, making them
    /// stick to only one database.
    pub(crate) omni_index: usize,

    /// Desired session role from the `pgdog.role` startup parameter.
    pub(crate) role: Option<SessionRole>,
}

impl Default for Sticky {
    fn default() -> Self {
        Self::new()
    }
}

impl Sticky {
    /// Create new sticky config.
    pub(crate) fn new() -> Self {
        Self::from_params(&Parameters::default())
    }

    #[cfg(test)]
    pub(crate) fn new_test() -> Self {
        Self {
            omni_index: 1,
            role: None,
        }
    }

    /// Create Sticky from params.
    pub(crate) fn from_params(params: &Parameters) -> Self {
        let role = params
            .get("pgdog.role")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse().ok());

        Self {
            omni_index: rng().random_range(1..usize::MAX),
            role,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sticky() {
        let params = Parameters::default();
        assert!(Sticky::from_params(&params).role.is_none());

        for (attr, role) in [
            ("primary", Some(SessionRole::Primary)),
            ("replica", Some(SessionRole::Replica)),
            ("random", None),
        ] {
            let mut params = Parameters::default();
            params.insert("pgdog.role", attr);
            let sticky = Sticky::from_params(&params);
            assert_eq!(sticky.role, role);
        }
    }
}
