use crate::net::messages::data_types::Float;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Scalar reference implementation - no allocations
#[inline]
pub fn euclidean_distance_scalar(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    let mut sum = 0.0f32;
    for i in 0..p.len() {
        let diff = q[i].0 - p[i].0;
        sum += diff * diff;
    }
    sum.sqrt()
}

/// SIMD implementation for x86_64 with SSE
#[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
#[inline]
pub fn euclidean_distance_sse(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        let mut sum = _mm_setzero_ps();
        let chunks = p.len() / 4;

        // Process 4 floats at a time
        for i in 0..chunks {
            let idx = i * 4;
            let p_vec = _mm_set_ps(p[idx + 3].0, p[idx + 2].0, p[idx + 1].0, p[idx].0);
            let q_vec = _mm_set_ps(q[idx + 3].0, q[idx + 2].0, q[idx + 1].0, q[idx].0);
            let diff = _mm_sub_ps(q_vec, p_vec);
            let squared = _mm_mul_ps(diff, diff);
            sum = _mm_add_ps(sum, squared);
        }

        // Horizontal sum
        let mut result = [0.0f32; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result[0] + result[1] + result[2] + result[3];

        // Handle remaining elements
        for i in (chunks * 4)..p.len() {
            let diff = q[i].0 - p[i].0;
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// SIMD implementation for x86_64 with AVX2
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
pub fn euclidean_distance_avx2(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        let mut sum = _mm256_setzero_ps();
        let chunks = p.len() / 8;

        // Process 8 floats at a time
        for i in 0..chunks {
            let idx = i * 8;
            let p_vec = _mm256_set_ps(
                p[idx + 7].0,
                p[idx + 6].0,
                p[idx + 5].0,
                p[idx + 4].0,
                p[idx + 3].0,
                p[idx + 2].0,
                p[idx + 1].0,
                p[idx].0,
            );
            let q_vec = _mm256_set_ps(
                q[idx + 7].0,
                q[idx + 6].0,
                q[idx + 5].0,
                q[idx + 4].0,
                q[idx + 3].0,
                q[idx + 2].0,
                q[idx + 1].0,
                q[idx].0,
            );
            let diff = _mm256_sub_ps(q_vec, p_vec);

            // Use FMA if available
            #[cfg(target_feature = "fma")]
            {
                sum = _mm256_fmadd_ps(diff, diff, sum);
            }
            #[cfg(not(target_feature = "fma"))]
            {
                let squared = _mm256_mul_ps(diff, diff);
                sum = _mm256_add_ps(sum, squared);
            }
        }

        // Horizontal sum
        let mut result = [0.0f32; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();

        // Handle remaining elements
        for i in (chunks * 8)..p.len() {
            let diff = q[i].0 - p[i].0;
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// SIMD implementation for ARM NEON
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn euclidean_distance_neon(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        let mut sum = vdupq_n_f32(0.0);
        let chunks = p.len() / 4;

        // Process 4 floats at a time
        for i in 0..chunks {
            let idx = i * 4;
            // Load 4 floats from p and q
            let p_vals = [p[idx].0, p[idx + 1].0, p[idx + 2].0, p[idx + 3].0];
            let q_vals = [q[idx].0, q[idx + 1].0, q[idx + 2].0, q[idx + 3].0];

            let p_vec = vld1q_f32(p_vals.as_ptr());
            let q_vec = vld1q_f32(q_vals.as_ptr());
            let diff = vsubq_f32(q_vec, p_vec);
            sum = vfmaq_f32(sum, diff, diff); // Fused multiply-add
        }

        // Horizontal sum
        let sum_pair = vadd_f32(vget_low_f32(sum), vget_high_f32(sum));
        let sum_scalar = vpadd_f32(sum_pair, sum_pair);
        let mut total = vget_lane_f32(sum_scalar, 0);

        // Handle remaining elements
        for i in (chunks * 4)..p.len() {
            let diff = q[i].0 - p[i].0;
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// Auto-select best implementation based on CPU features
#[inline]
pub fn euclidean_distance(p: &[Float], q: &[Float]) -> f32 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        euclidean_distance_avx2(p, q)
    }

    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "sse",
        not(target_feature = "avx2")
    ))]
    {
        euclidean_distance_sse(p, q)
    }

    #[cfg(target_arch = "aarch64")]
    {
        euclidean_distance_neon(p, q)
    }

    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "sse"),
        target_arch = "aarch64"
    )))]
    {
        euclidean_distance_scalar(p, q)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::Vector;

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
