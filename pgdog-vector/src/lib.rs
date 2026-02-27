use std::{fmt::Debug, ops::Deref};

use schemars::JsonSchema;
use serde::{
    de::{self, Visitor},
    ser::SerializeSeq,
    Deserialize, Serialize,
};

pub mod distance_simd_rust;
pub mod float;

pub use float::*;

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, JsonSchema)]
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

    /// Compute L2 distance between the vectors.
    pub fn distance_l2(&self, other: &Self) -> f32 {
        Distance::Euclidean(self, other).distance()
    }
}

pub enum Distance<'a> {
    Euclidean(&'a Vector, &'a Vector),
}

impl Distance<'_> {
    pub fn distance(&self) -> f32 {
        match self {
            Self::Euclidean(p, q) => {
                assert_eq!(p.len(), q.len());
                // Avoids temporary array allocations by working directly with the Float slices
                distance_simd_rust::euclidean_distance(p, q)
            }
        }
    }

    // Fallback implementation for testing
    pub fn distance_scalar(&self) -> f32 {
        match self {
            Self::Euclidean(p, q) => {
                assert_eq!(p.len(), q.len());
                // Use scalar implementation
                distance_simd_rust::euclidean_distance_scalar(p, q)
            }
        }
    }
}

impl Debug for Vector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.values.len() > 3 {
            f.debug_struct("Vector")
                .field(
                    "values",
                    &format!(
                        "[{}..{}]",
                        self.values[0],
                        self.values[self.values.len() - 1]
                    ),
                )
                .finish()
        } else {
            f.debug_struct("Vector")
                .field("values", &self.values)
                .finish()
        }
    }
}

impl Deref for Vector {
    type Target = Vec<Float>;

    fn deref(&self) -> &Self::Target {
        &self.values
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

#[derive(Debug)]
pub struct Centroids<'a> {
    centroids: &'a [Vector],
}

impl Centroids<'_> {
    /// Find the shards with the closest centroids,
    /// according to the number of probes.
    pub fn shard(&self, vector: &Vector, shards: usize, probes: usize) -> Vec<usize> {
        let mut selected = vec![];
        let mut centroids = self.centroids.iter().enumerate().collect::<Vec<_>>();
        centroids.sort_by(|(_, a), (_, b)| {
            a.distance_l2(vector)
                .partial_cmp(&b.distance_l2(vector))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let centroids = centroids.into_iter().take(probes);
        for (i, _) in centroids {
            selected.push(i % shards);
        }

        selected
    }
}

impl<'a> From<&'a Vec<Vector>> for Centroids<'a> {
    fn from(centroids: &'a Vec<Vector>) -> Self {
        Centroids { centroids }
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

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_euclidean() {
        let v1 = Vector::from(&[1.0, 2.0, 3.0][..]);
        let v2 = Vector::from(&[1.5, 2.0, 3.0][..]);
        let distance = Distance::Euclidean(&v1, &v2).distance();
        assert_eq!(distance, 0.5);
    }

    #[test]
    fn test_simd_features() {
        println!("SIMD features available:");
        #[cfg(target_arch = "x86_64")]
        {
            println!("  x86_64 architecture detected");
            #[cfg(target_feature = "sse")]
            println!("  SSE: enabled");
            #[cfg(target_feature = "avx2")]
            println!("  AVX2: enabled");
            #[cfg(target_feature = "fma")]
            println!("  FMA: enabled");
        }
        #[cfg(target_arch = "aarch64")]
        {
            println!("  ARM64 architecture detected");
            println!("  NEON: enabled (always available on ARM64)");
        }
    }

    #[test]
    fn test_simd_vs_scalar() {
        // Test small vectors
        let v1 = Vector::from(&[3.0, 4.0][..]);
        let v2 = Vector::from(&[0.0, 0.0][..]);
        let simd_dist = Distance::Euclidean(&v1, &v2).distance();
        let scalar_dist = Distance::Euclidean(&v1, &v2).distance_scalar();
        assert!((simd_dist - scalar_dist).abs() < 1e-6);
        assert!((simd_dist - 5.0).abs() < 1e-6);

        // Test medium vectors
        let v3: Vec<f32> = (0..128).map(|i| i as f32).collect();
        let v4: Vec<f32> = (0..128).map(|i| (i + 1) as f32).collect();
        let v3 = Vector::from(v3);
        let v4 = Vector::from(v4);
        let simd_dist = Distance::Euclidean(&v3, &v4).distance();
        let scalar_dist = Distance::Euclidean(&v3, &v4).distance_scalar();
        assert!((simd_dist - scalar_dist).abs() < 1e-4);
    }

    #[test]
    fn test_openai_embedding_size() {
        // Test with OpenAI text-embedding-3-small dimension (1536)
        let v1: Vec<f32> = (0..1536).map(|i| (i as f32) * 0.001).collect();
        let v2: Vec<f32> = (0..1536).map(|i| (i as f32) * 0.001 + 0.5).collect();
        let v1 = Vector::from(v1);
        let v2 = Vector::from(v2);

        let simd_dist = Distance::Euclidean(&v1, &v2).distance();
        let scalar_dist = Distance::Euclidean(&v1, &v2).distance_scalar();

        // Check that both implementations produce very similar results
        let relative_error = ((simd_dist - scalar_dist).abs() / scalar_dist).abs();
        assert!(
            relative_error < 1e-5,
            "Relative error: {}, SIMD: {}, Scalar: {}",
            relative_error,
            simd_dist,
            scalar_dist
        );
    }

    #[test]
    fn test_special_values() {
        // Test with NaN
        let v_nan = Vector::from(vec![Float(f32::NAN), Float(1.0)]);
        let v_normal = Vector::from(&[1.0, 1.0][..]);
        let simd_dist = Distance::Euclidean(&v_nan, &v_normal).distance();
        let scalar_dist = Distance::Euclidean(&v_nan, &v_normal).distance_scalar();
        assert!(simd_dist.is_nan());
        assert!(scalar_dist.is_nan());

        // Test with Infinity
        let v_inf = Vector::from(vec![Float(f32::INFINITY), Float(1.0)]);
        let simd_dist = Distance::Euclidean(&v_inf, &v_normal).distance();
        let scalar_dist = Distance::Euclidean(&v_inf, &v_normal).distance_scalar();
        assert!(simd_dist.is_infinite());
        assert!(scalar_dist.is_infinite());
    }

    #[test]
    fn test_zero_distance() {
        let v1 = Vector::from(&[1.0, 2.0, 3.0, 4.0, 5.0][..]);
        let simd_dist = Distance::Euclidean(&v1, &v1).distance();
        let scalar_dist = Distance::Euclidean(&v1, &v1).distance_scalar();
        assert_eq!(simd_dist, 0.0);
        assert_eq!(scalar_dist, 0.0);
    }

    #[test]
    fn test_various_sizes() {
        // Test various vector sizes to ensure correct handling of tail elements
        for size in [
            1, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 512, 1024,
        ] {
            let v1: Vec<f32> = (0..size).map(|i| i as f32).collect();
            let v2: Vec<f32> = (0..size).map(|i| (i * 2) as f32).collect();
            let v1 = Vector::from(v1);
            let v2 = Vector::from(v2);

            let simd_dist = Distance::Euclidean(&v1, &v2).distance();
            let scalar_dist = Distance::Euclidean(&v1, &v2).distance_scalar();

            let relative_error = if scalar_dist != 0.0 {
                ((simd_dist - scalar_dist).abs() / scalar_dist).abs()
            } else {
                (simd_dist - scalar_dist).abs()
            };

            assert!(
                relative_error < 1e-5,
                "Size {}: relative error {}, SIMD: {}, Scalar: {}",
                size,
                relative_error,
                simd_dist,
                scalar_dist
            );
        }
    }

    #[test]
    fn test_vector_distance_with_special_values() {
        // Test distance calculation with normal values
        let v1 = Vector::from(&[3.0, 4.0][..]);
        let v2 = Vector::from(&[0.0, 0.0][..]);
        let distance = v1.distance_l2(&v2);
        assert_eq!(distance, 5.0); // 3-4-5 triangle

        // Test distance with NaN
        let v_nan = Vector::from(vec![Float(f32::NAN), Float(1.0)]);
        let v_normal = Vector::from(&[1.0, 1.0][..]);
        let distance_nan = v_nan.distance_l2(&v_normal);
        assert!(distance_nan.is_nan());

        // Test distance with Infinity
        let v_inf = Vector::from(vec![Float(f32::INFINITY), Float(1.0)]);
        let distance_inf = v_inf.distance_l2(&v_normal);
        assert!(distance_inf.is_infinite());
    }
}
