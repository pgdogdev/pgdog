//! Sticky settings for clients that override
//! default routing behavior determined by the query parser.

use pgdog_config::Role;
use rand::{thread_rng, Rng};

use crate::net::{parameter::ParameterValue, Parameters};

#[derive(Debug, Clone, Copy)]
pub struct Sticky {
    /// Which shard to use for omnisharded queries, making them
    /// stick to only one database.
    pub omni_index: usize,

    /// Desired database role. This comes from `target_session_attrs`
    /// provided by the client.
    pub role: Option<Role>,
}

impl Sticky {
    /// Create new sticky config.
    pub fn new() -> Self {
        Self::from_params(&Parameters::default())
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            omni_index: 1,
            role: None,
        }
    }

    /// Create Sticky from params.
    pub fn from_params(params: &Parameters) -> Self {
        let role = params
            .get("pgdog.role")
            .map(|value| match value {
                ParameterValue::String(value) => match value.as_str() {
                    "primary" => Some(Role::Primary),
                    "replica" => Some(Role::Replica),
                    _ => None,
                },
                _ => None,
            })
            .flatten();

        Self {
            omni_index: thread_rng().gen_range(1..usize::MAX),
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
            ("primary", Some(Role::Primary)),
            ("replica", Some(Role::Replica)),
            ("random", None),
        ] {
            let mut params = Parameters::default();
            params.insert("pgdog.role", attr);
            let sticky = Sticky::from_params(&params);
            assert_eq!(sticky.role, role);
        }
    }
}
