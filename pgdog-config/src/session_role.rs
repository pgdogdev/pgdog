use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Database role a client session wants its queries routed to.
///
/// Could be specified with:
/// - `user.role` config
/// - startup parameters
/// - SET/SET LOCAL commands
/// - inline sql comment
///
/// <https://docs.pgdog.dev/features/load-balancer/manual-routing/#parameters>
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    derive_more::Display,
    derive_more::FromStr,
)]
#[serde(rename_all = "snake_case")]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum SessionRole {
    /// Send queries to the primary.
    Primary,
    /// Send queries to the replicas.
    Replica,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_session_role_from_str() {
        assert_eq!(
            "primary".parse::<SessionRole>().ok(),
            Some(SessionRole::Primary)
        );
        assert_eq!(
            "replica".parse::<SessionRole>().ok(),
            Some(SessionRole::Replica)
        );
        assert!("auto".parse::<SessionRole>().is_err());
    }
}
