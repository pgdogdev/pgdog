/// Parameters that we don't care if the client changes
/// or if they are different from the server parameters.
pub(super) const UNTRACKED_PARAMS: &[&'static str] = &[
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
