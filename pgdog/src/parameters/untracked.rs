/// Parameters that we don't care if the client changes
/// or if they are different from the server parameters.
const UNTRACKED_PARAMS: &[&str] = &[
    "database",
    "user",
    "client_encoding",
    "replication",
    "is_superuser",
    "server_version",
    "server_encoding",
    "integer_datetimes",
    "session_authorization",
    "in_hot_standby",
    "pgdog.role",
    "pgdog.shard",
    "pgdog.sharding_key",
];

pub(super) fn is_untracked(name: &str) -> bool {
    UNTRACKED_PARAMS
        .iter()
        .any(|untracked| name.eq_ignore_ascii_case(untracked))
}
