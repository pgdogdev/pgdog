#[derive(Debug, Display, Clone, Copy, Default, PartialEq)]
#[display(rename_all = "snake_case")]
pub(crate) enum ConnectReason {
    LsnCheck,
    BelowMin,
    ClientWaiting,
    Resharding,
    PubSub,
    Probe,
    Healthcheck,
    #[default]
    Other,
}
