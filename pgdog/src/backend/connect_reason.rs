use std::fmt::Display;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum ConnectReason {
    LsnCheck,
    BelowMin,
    ClientWaiting,
    Replication,
    PubSub,
    Probe,
    Healthcheck,
    #[default]
    Other,
}

impl Display for ConnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::LsnCheck => "lsn check",
            Self::BelowMin => "min",
            Self::ClientWaiting => "client",
            Self::Replication => "replication",
            Self::PubSub => "pub/sub",
            Self::Probe => "probe",
            Self::Healthcheck => "healthcheck",
            Self::Other => "other",
        };

        write!(f, "{}", reason)
    }
}
