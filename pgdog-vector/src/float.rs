use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};

/// Wrapper type for f32 that implements Ord for PostgreSQL compatibility
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Float(pub f32);

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        // PostgreSQL ordering: NaN is greater than all other values
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal),
        }
    }
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        // PostgreSQL treats NaN as equal to NaN for indexing purposes
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for Float {}

impl Hash for Float {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            // All NaN values hash to the same value
            0u8.hash(state);
        } else {
            // Use bit representation for consistent hashing
            self.0.to_bits().hash(state);
        }
    }
}

impl Display for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_nan() {
            write!(f, "NaN")
        } else if self.0.is_infinite() {
            if self.0.is_sign_positive() {
                write!(f, "Infinity")
            } else {
                write!(f, "-Infinity")
            }
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<f32> for Float {
    fn from(value: f32) -> Self {
        Float(value)
    }
}

impl From<Float> for f32 {
    fn from(value: Float) -> Self {
        value.0
    }
}
