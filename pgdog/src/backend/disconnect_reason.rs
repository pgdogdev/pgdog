use std::fmt::Display;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum DisconnectReason {
    Idle,
    Old,
    Error,
    Offline,
    ForceClose,
    ReplicationMode,
    OutOfSync,
    Unhealthy,
    Healthcheck,
    CredentialsRefresh,
    CredentialsCheck,
    ServerClosed,
    #[default]
    Other,
}

impl Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::Idle => "idle",
            Self::Old => "max age",
            Self::Error => "error",
            Self::Other => "other",
            Self::ForceClose => "force_close",
            Self::Offline => "pool_offline",
            Self::OutOfSync => "out of sync",
            Self::ReplicationMode => "in_replication_mode",
            Self::Unhealthy => "unhealthy",
            Self::Healthcheck => "standalone_healthcheck",
            Self::CredentialsRefresh => "credentials_refresh",
            Self::ServerClosed => "server_closed",
            Self::CredentialsCheck => "credentials_check",
        };

        write!(f, "{}", reason)
    }
}
