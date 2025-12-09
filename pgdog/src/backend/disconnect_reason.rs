use std::fmt::Display;

#[derive(Debug, Clone, Copy, Default)]
pub enum DisconnectReason {
    Idle,
    Old,
    Error,
    Offline,
    ForceClose,
    Paused,
    ReplicationMode,
    OutOfSync,
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
            Self::Paused => "pool paused",
            Self::Offline => "pool offline",
            Self::OutOfSync => "out of sync",
            Self::ReplicationMode => "in replication mode",
        };

        write!(f, "{}", reason)
    }
}
