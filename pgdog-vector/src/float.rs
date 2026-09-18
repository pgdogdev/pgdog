use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Wrapper type for f32 that implements Ord for PostgreSQL compatibility
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
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
        } else if self.0 == 0.0 {
            // 0.0 and -0.0 compare equal but have different bit patterns,
            // so they must hash to the same value. Postgres normalizes the
            // sign of zero the same way, in hashfloat4.
            0.0_f32.to_bits().hash(state);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(float: Float) -> u64 {
        let mut hasher = DefaultHasher::new();
        float.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_negative_zero_hashes_like_zero() {
        assert_eq!(Float(0.0), Float(-0.0));
        assert_eq!(hash_of(Float(0.0)), hash_of(Float(-0.0)));

        let mut set = HashSet::new();
        set.insert(Float(0.0));
        set.insert(Float(-0.0));
        assert_eq!(set.len(), 1);
        assert!(set.contains(&Float(-0.0)));
    }

    #[test]
    fn test_distinct_values_still_hash_apart() {
        assert_ne!(hash_of(Float(1.0)), hash_of(Float(-1.0)));
        assert_ne!(hash_of(Float(0.0)), hash_of(Float(1.0)));
        assert_ne!(hash_of(Float(f32::NAN)), hash_of(Float(0.0)));
    }
}
