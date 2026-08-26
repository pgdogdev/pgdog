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
            Self::ForceClose => "force close",
            Self::Offline => "pool offline",
            Self::OutOfSync => "out of sync",
            Self::ReplicationMode => "in replication mode",
            Self::Unhealthy => "unhealthy",
            Self::Healthcheck => "standalone healthcheck",
            Self::CredentialsRefresh => "credentials refresh",
            Self::ServerClosed => "server closed",
        };

        write!(f, "{}", reason)
    }
}
