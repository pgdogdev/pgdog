use pg_raw_parse::raw::SQLValueFunctionOp;

use crate::frontend::router::parser::rewrite::statement::{
    Error, non_deterministic_funcs::NDFunctionType,
};

/// TODO: Docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UUIDFunctionType {
    Uuidv4,
    Uuidv7, // TODO: Accept a smallint param.
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

    /// If the type has a parameter (for precision), return the same type with that parameter.
    /// TODO: Handle this for UUIDv7.
    pub(super) fn with_param(self, _precision: u8) -> Self {
        self
    }

    pub(super) fn format(self) -> Result<String, Error> {
        // TODO: Generate a random UUIDv4 / UUIDv7 based on the `UUIDFunctionType`
        todo!()
    }
}
