use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use serde::{
    de::{self, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

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

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
#[repr(C)]
pub struct Vector {
    pub values: Vec<Float>,
}

impl Vector {
    /// Length of the vector.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Is the vector empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct VectorVisitor;

impl<'de> Visitor<'de> for VectorVisitor {
    type Value = Vector;

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut results = vec![];
        while let Some(n) = seq.next_element::<f64>()? {
            results.push(n);
        }

        Ok(Vector::from(results.as_slice()))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected a list of floating points")
    }
}

impl<'de> Deserialize<'de> for Vector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(VectorVisitor)
    }
}

impl Serialize for Vector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for v in &self.values {
            seq.serialize_element(v)?;
        }
        seq.end()
    }
}

impl From<&[f64]> for Vector {
    fn from(value: &[f64]) -> Self {
        Self {
            values: value.iter().map(|v| Float(*v as f32)).collect(),
        }
    }
}

impl From<&[f32]> for Vector {
    fn from(value: &[f32]) -> Self {
        Self {
            values: value.iter().map(|v| Float(*v)).collect(),
        }
    }
}

impl From<Vec<f32>> for Vector {
    fn from(value: Vec<f32>) -> Self {
        Self {
            values: value.into_iter().map(Float::from).collect(),
        }
    }
}

impl From<Vec<f64>> for Vector {
    fn from(value: Vec<f64>) -> Self {
        Self {
            values: value.into_iter().map(|v| Float(v as f32)).collect(),
        }
    }
}

impl From<Vec<Float>> for Vector {
    fn from(value: Vec<Float>) -> Self {
        Self { values: value }
    }
}
