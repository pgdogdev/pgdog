pub use pgdog_vector::distance_simd_rust::*;

#[cfg(test)]
mod tests {

    use pgdog_vector::distance_simd_rust::*;
    use pgdog_vector::{Float, Vector};

    #[test]
    fn test_no_allocations() {
        // This test verifies we're not allocating
        let v1 = Vector::from(&[1.0, 2.0, 3.0][..]);
        let v2 = Vector::from(&[4.0, 5.0, 6.0][..]);

        // These calls should not allocate
        let dist = euclidean_distance(&v1, &v2);
        assert!(dist > 0.0);
    }

    #[test]
    fn test_correctness() {
        let v1: Vec<Float> = vec![Float(3.0), Float(4.0)];
        let v2: Vec<Float> = vec![Float(0.0), Float(0.0)];

        let dist_scalar = euclidean_distance_scalar(&v1, &v2);
        let dist_simd = euclidean_distance(&v1, &v2);

        assert!((dist_scalar - 5.0).abs() < 1e-6);
        assert!((dist_simd - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_large_vectors() {
        // Test with 1536-dimensional vectors (OpenAI embeddings)
        let v1: Vec<Float> = (0..1536).map(|i| Float(i as f32 * 0.001)).collect();
        let v2: Vec<Float> = (0..1536).map(|i| Float(i as f32 * 0.001 + 0.5)).collect();

        let dist_scalar = euclidean_distance_scalar(&v1, &v2);
        let dist_simd = euclidean_distance(&v1, &v2);

        let relative_error = ((dist_simd - dist_scalar).abs() / dist_scalar).abs();
        assert!(relative_error < 1e-5);
    }
}
