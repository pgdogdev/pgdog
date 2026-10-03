use chrono::Utc;
use pg_raw_parse::raw::SQLValueFunctionOp;
use uuid::{ContextV7, Timestamp, Uuid};

use crate::frontend::router::parser::rewrite::statement::{
    Error, non_deterministic_funcs::NDFunctionType,
};

/// Represents the kind of `UUIDFunction` that we're re-writing.
/// <https://www.postgresql.org/docs/current/functions-uuid.html>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UUIDFunctionType {
    Uuidv4,
    Uuidv7, //TODO: Postgres supports a parameter for an interval to be specified to shift the timestamp.
    GenRandomUuid,
}

impl UUIDFunctionType {
    /// For easy iteration over all enum variants for pattern matching.
    pub(super) const ALL_VARIANTS: [NDFunctionType; 3] = [
        NDFunctionType::UUIDFunction(Self::Uuidv4),
        NDFunctionType::UUIDFunction(Self::Uuidv7),
        NDFunctionType::UUIDFunction(Self::GenRandomUuid),
    ];

    /// Convert `SQLValueFunctionOp` (e.g. current_date, current_time... non ()) to `UUIDFunctionType`
    /// There are no such cases for UUID functions.
    pub(super) fn from_sql_value_function(
        _op: SQLValueFunctionOp::Type,
        _typmod: i32,
    ) -> Option<Self> {
        None
    }

    /// Postgres formatted String to match against Client-provided names in query.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Uuidv4 => "uuidv4",
            Self::Uuidv7 => "uuidv7",
            Self::GenRandomUuid => "gen_random_uuid",
        }
    }

    /// If the type has a parameter, return the same type with that parameter.
    /// TODO: Support intervals for uuidv7 (Param enum to generalize precision / interval)
    pub(super) fn with_param(self) -> Self {
        self
    }

    /// Generate a random UUIDv4 / UUIDv7 based on the `UUIDFunctionType`
    pub(super) fn format(self) -> Result<String, Error> {
        Ok(match self {
            Self::Uuidv4 | Self::GenRandomUuid => Uuid::new_v4().to_string(),
            Self::Uuidv7 => {
                // I considered re-using `QueryTimestamps` (which stores statement and transaction times), however,
                // what if we have multiple function calls within the same INSERT? That would mean generating the same
                // UUIDs (as it's deterministic if the input time is the same), meaning we have to account for that
                // by always generating a new time.
                let current_time = Utc::now();
                Uuid::new_v7(Timestamp::from_unix(
                    ContextV7::new(),
                    current_time.timestamp_millis() as u64,
                    current_time.timestamp_subsec_nanos(),
                ))
                .to_string()
            }
        })
    }
}
