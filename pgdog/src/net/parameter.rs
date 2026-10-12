//! Startup parameter.

pub(crate) use crate::parameters::ParameterValue;

/// Startup parameter.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Parameter {
    /// Parameter name.
    pub(crate) name: String,
    /// Parameter value.
    pub(crate) value: ParameterValue,
}

impl<T: ToString> From<(T, T)> for Parameter {
    fn from(value: (T, T)) -> Self {
        Self {
            name: value.0.to_string(),
            value: ParameterValue::String(value.1.to_string()),
        }
    }
}
