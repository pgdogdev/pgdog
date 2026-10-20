//! Normalize parameter values without changing the session's parameter store.

use pg_internals::guc::{DateStyleError, normalize_datestyle};
use std::borrow::Cow;

use super::{ParameterStore, ParameterValue};
use crate::{frontend::SetParam, net::ErrorResponse};

/// Tracks effective values while normalizing successive assignments in a batch.
pub(crate) struct ParameterNormalizer<'a> {
    datestyle: String,
    reset_datestyle: &'a str,
}

impl<'a> ParameterNormalizer<'a> {
    pub(crate) fn new(store: &'a ParameterStore) -> Self {
        let reset_datestyle = store
            .get_startup("DateStyle")
            .and_then(ParameterValue::as_str)
            .unwrap_or("ISO, MDY");
        Self {
            datestyle: store.get_default("DateStyle", reset_datestyle).to_owned(),
            reset_datestyle,
        }
    }

    /// Normalize one assignment in place, using prior assignments as context.
    /// The caller skips SET LOCAL outside a transaction before calling this.
    pub(crate) fn normalize_datestyle(&mut self, param: &mut SetParam) -> Option<ErrorResponse> {
        if !param.name.eq_ignore_ascii_case("DateStyle") {
            return None;
        }
        param.name = "DateStyle".into();
        let Some(value) = &param.value else {
            self.datestyle = self.reset_datestyle.to_owned();
            return None;
        };
        // PostgreSQL joins SET datestyle = ISO, MDY arguments before checking
        // the value. String assignments can be borrowed directly.
        let value = match value {
            ParameterValue::String(value) => Cow::Borrowed(value.as_str()),
            ParameterValue::Tuple(values) => Cow::Owned(values.join(", ")),
        };
        match normalize_datestyle(&value, &self.datestyle, self.reset_datestyle) {
            Ok(normalized) => {
                self.datestyle = normalized.clone();
                param.value = Some(normalized.into());
                None
            }
            Err(error) => Some(ErrorResponse {
                code: "22023".into(),
                message: format!("invalid value for parameter \"DateStyle\": \"{value}\""),
                detail: Some(match error {
                    DateStyleError::Invalid { detail, .. } => detail,
                    error => error.to_string(),
                }),
                ..Default::default()
            }),
        }
    }
}
