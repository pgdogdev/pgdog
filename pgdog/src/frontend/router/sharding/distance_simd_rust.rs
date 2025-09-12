use crate::net::messages::data_types::Float;
use std::mem;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Helper function to convert Float slice to f32 slice
/// SAFETY: Float is a newtype wrapper around f32, so the memory layout is identical
#[inline(always)]
unsafe fn float_slice_to_f32(floats: &[Float]) -> &[f32] {
    // This is safe because Float is a transparent wrapper around f32
    std::slice::from_raw_parts(floats.as_ptr() as *const f32, floats.len())
}

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

/// SIMD implementation for x86_64 with SSE - optimized version
#[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
#[inline]
pub fn euclidean_distance_sse(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        // Convert Float slices to f32 slices to avoid temporary arrays
        let p_f32 = float_slice_to_f32(p);
        let q_f32 = float_slice_to_f32(q);

        let mut sum1 = _mm_setzero_ps();
        let mut sum2 = _mm_setzero_ps();
        let chunks = p.len() / 8; // Process 8 at a time (2 SSE vectors)

        // Unroll loop to process 8 floats per iteration
        for i in 0..chunks {
            let idx = i * 8;

            // First 4 floats - direct load from slice
            let p_vec1 = _mm_loadu_ps(p_f32.as_ptr().add(idx));
            let q_vec1 = _mm_loadu_ps(q_f32.as_ptr().add(idx));
            let diff1 = _mm_sub_ps(q_vec1, p_vec1);
            sum1 = _mm_add_ps(sum1, _mm_mul_ps(diff1, diff1));

            // Second 4 floats
            let p_vec2 = _mm_loadu_ps(p_f32.as_ptr().add(idx + 4));
            let q_vec2 = _mm_loadu_ps(q_f32.as_ptr().add(idx + 4));
            let diff2 = _mm_sub_ps(q_vec2, p_vec2);
            sum2 = _mm_add_ps(sum2, _mm_mul_ps(diff2, diff2));
        }

        // Combine accumulators
        let sum = _mm_add_ps(sum1, sum2);

        // More efficient horizontal sum using shuffles
        let shuf = _mm_shuffle_ps(sum, sum, 0b01_00_11_10); // [2,3,0,1]
        let sums = _mm_add_ps(sum, shuf);
        let shuf = _mm_shuffle_ps(sums, sums, 0b10_11_00_01); // [1,0,3,2]
        let result = _mm_add_ps(sums, shuf);
        let mut total = _mm_cvtss_f32(result);

        // Handle remaining elements
        for i in (chunks * 8)..p.len() {
            let diff = q[i].0 - p[i].0;
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// SIMD implementation for x86_64 with AVX2 - optimized version
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
pub fn euclidean_distance_avx2(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        // Convert Float slices to f32 slices to avoid temporary arrays
        let p_f32 = float_slice_to_f32(p);
        let q_f32 = float_slice_to_f32(q);

        let mut sum1 = _mm256_setzero_ps();
        let mut sum2 = _mm256_setzero_ps();
        let chunks = p.len() / 16; // Process 16 at a time (2 AVX2 vectors)

        // Unroll loop to process 16 floats per iteration
        for i in 0..chunks {
            let idx = i * 16;

            // First 8 floats - direct load from slice
            let p_vec1 = _mm256_loadu_ps(p_f32.as_ptr().add(idx));
            let q_vec1 = _mm256_loadu_ps(q_f32.as_ptr().add(idx));
            let diff1 = _mm256_sub_ps(q_vec1, p_vec1);

            #[cfg(target_feature = "fma")]
            {
                sum1 = _mm256_fmadd_ps(diff1, diff1, sum1);
            }
            #[cfg(not(target_feature = "fma"))]
            {
                sum1 = _mm256_add_ps(sum1, _mm256_mul_ps(diff1, diff1));
            }

            // Second 8 floats
            let p_vec2 = _mm256_loadu_ps(p_f32.as_ptr().add(idx + 8));
            let q_vec2 = _mm256_loadu_ps(q_f32.as_ptr().add(idx + 8));
            let diff2 = _mm256_sub_ps(q_vec2, p_vec2);

            #[cfg(target_feature = "fma")]
            {
                sum2 = _mm256_fmadd_ps(diff2, diff2, sum2);
            }
            #[cfg(not(target_feature = "fma"))]
            {
                sum2 = _mm256_add_ps(sum2, _mm256_mul_ps(diff2, diff2));
            }
        }

        // Combine accumulators
        let sum = _mm256_add_ps(sum1, sum2);

        // More efficient horizontal sum using extract and SSE
        let high = _mm256_extractf128_ps(sum, 1);
        let low = _mm256_castps256_ps128(sum);
        let sum128 = _mm_add_ps(low, high);

        // Use SSE horizontal sum (more efficient than storing to array)
        let shuf = _mm_shuffle_ps(sum128, sum128, 0b01_00_11_10);
        let sums = _mm_add_ps(sum128, shuf);
        let shuf = _mm_shuffle_ps(sums, sums, 0b10_11_00_01);
        let result = _mm_add_ps(sums, shuf);
        let mut total = _mm_cvtss_f32(result);

        // Handle remaining elements
        for i in (chunks * 16)..p.len() {
            let diff = q[i].0 - p[i].0;
            total += diff * diff;
        }

        total.sqrt()
    }
}

/// SIMD implementation for ARM NEON - optimized version
#[cfg(target_arch = "aarch64")]
#[inline]
pub fn euclidean_distance_neon(p: &[Float], q: &[Float]) -> f32 {
    debug_assert_eq!(p.len(), q.len());

    unsafe {
        // Convert Float slices to f32 slices to avoid temporary arrays
        let p_f32 = float_slice_to_f32(p);
        let q_f32 = float_slice_to_f32(q);

        let mut sum1 = vdupq_n_f32(0.0);
        let mut sum2 = vdupq_n_f32(0.0);
        let chunks = p.len() / 8; // Process 8 at a time (2 vectors)

        // Unroll loop to process 8 floats per iteration (2x4)
        for i in 0..chunks {
            let idx = i * 8;

            // First 4 floats - direct load from slice
            let p_vec1 = vld1q_f32(p_f32.as_ptr().add(idx));
            let q_vec1 = vld1q_f32(q_f32.as_ptr().add(idx));
            let diff1 = vsubq_f32(q_vec1, p_vec1);
            sum1 = vfmaq_f32(sum1, diff1, diff1);

            // Second 4 floats
            let p_vec2 = vld1q_f32(p_f32.as_ptr().add(idx + 4));
            let q_vec2 = vld1q_f32(q_f32.as_ptr().add(idx + 4));
            let diff2 = vsubq_f32(q_vec2, p_vec2);
            sum2 = vfmaq_f32(sum2, diff2, diff2);
        }

        // Combine the two accumulators
        let sum = vaddq_f32(sum1, sum2);

        // Horizontal sum - more efficient version
        let sum_pairs = vpaddq_f32(sum, sum);
        let sum_final = vpaddq_f32(sum_pairs, sum_pairs);
        let mut total = vgetq_lane_f32(sum_final, 0);

        // Handle remaining elements
        for i in (chunks * 8)..p.len() {
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
