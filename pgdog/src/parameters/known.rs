#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnownParameter {
    TimeZone,
    SearchPath,
    ApplicationName,
    PgdogShard,
    PgdogShardingKey,
    PgdogRole,
}

impl KnownParameter {
    pub(crate) const ALL: [Self; 6] = [
        Self::TimeZone,
        Self::SearchPath,
        Self::ApplicationName,
        Self::PgdogShard,
        Self::PgdogShardingKey,
        Self::PgdogRole,
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::TimeZone => "TimeZone",
            Self::SearchPath => "search_path",
            Self::ApplicationName => "application_name",
            Self::PgdogShard => "pgdog.shard",
            Self::PgdogShardingKey => "pgdog.sharding_key",
            Self::PgdogRole => "pgdog.role",
        }
    }

    pub(super) const fn index(self) -> usize {
        self as usize
    }
}
