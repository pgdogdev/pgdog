use crate::frontend::router::sharding::distance_simd_rust;
use crate::net::messages::{data_types::Float, Vector};
use std::time::{Duration, Instant};

fn benchmark_implementation(name: &str, p: &[Float], q: &[Float], iterations: usize) -> Duration {
    let start = Instant::now();

    // Use a simple loop to prevent compiler optimizations from eliminating the computation
    let mut sum = 0.0f32;
    for _ in 0..iterations {
        let dist = if name == "Scalar" {
            distance_simd_rust::euclidean_distance_scalar(p, q)
        } else {
            distance_simd_rust::euclidean_distance(p, q)
        };
        // Prevent optimization by using the result
        sum += dist * 0.00001;
    }

    // Use the sum to prevent dead code elimination
    if sum > 1000000.0 {
        println!("Unexpected sum: {}", sum);
    }

    start.elapsed()
}

pub fn run_benchmark() {
    println!("\n=== SIMD Distance Performance Benchmark ===");
    println!("Platform: {}", std::env::consts::ARCH);

    // Different vector sizes to test
    let sizes = [
        (128, "Small (128-dim)"),
        (512, "Medium (512-dim)"),
        (768, "BERT embeddings (768-dim)"),
        (1536, "OpenAI embeddings (1536-dim)"),
        (2048, "Large (2048-dim)"),
        (4096, "Extra large (4096-dim)"),
    ];

    println!("\nWarming up...");
    // Warmup to ensure consistent CPU frequency
    let warmup: Vec<Float> = (0..1000).map(|i| Float(i as f32)).collect();
    for _ in 0..10000 {
        let _ = distance_simd_rust::euclidean_distance(&warmup, &warmup);
    }

    println!("\nRunning benchmarks...");
    println!(
        "{:<30} {:>15} {:>15} {:>12} {:>12}",
        "Vector Size", "Scalar (ms)", "SIMD (ms)", "Speedup", "Throughput/s"
    );
    println!("{}", "-".repeat(90));

    for (size, description) in sizes.iter() {
        // Create test vectors
        let v1: Vec<Float> = (0..*size).map(|i| Float((i as f32) * 0.001)).collect();
        let v2: Vec<Float> = (0..*size)
            .map(|i| Float((i as f32) * 0.001 + 0.5))
            .collect();

        // Adjust iterations based on vector size for reasonable benchmark duration
        let iterations = match size {
            128..=512 => 100_000,
            513..=1536 => 50_000,
            1537..=2048 => 25_000,
            _ => 10_000,
        };

        // Benchmark scalar implementation
        let scalar_time = benchmark_implementation("Scalar", &v1, &v2, iterations);

        // Benchmark SIMD implementation
        let simd_time = benchmark_implementation("SIMD", &v1, &v2, iterations);

        // Calculate metrics
        let scalar_ms = scalar_time.as_secs_f64() * 1000.0;
        let simd_ms = simd_time.as_secs_f64() * 1000.0;
        let speedup = scalar_ms / simd_ms;
        let throughput = (iterations as f64) / simd_time.as_secs_f64();

        println!(
            "{:<30} {:>15.2} {:>15.2} {:>11.2}x {:>12.0}",
            description, scalar_ms, simd_ms, speedup, throughput
        );
    }

    // Test with actual Vector types to ensure no overhead
    println!("\n=== Testing with Vector type (1536-dim) ===");
    let v1 = Vector::from((0..1536).map(|i| (i as f32) * 0.001).collect::<Vec<f32>>());
    let v2 = Vector::from(
        (0..1536)
            .map(|i| (i as f32) * 0.001 + 0.5)
            .collect::<Vec<f32>>(),
    );

    let iterations = 50_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = crate::frontend::router::sharding::vector::Distance::Euclidean(&v1, &v2).distance();
    }
    let simd_vector_time = start.elapsed();

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = crate::frontend::router::sharding::vector::Distance::Euclidean(&v1, &v2)
            .distance_scalar();
    }
    let scalar_vector_time = start.elapsed();

    let speedup = scalar_vector_time.as_secs_f64() / simd_vector_time.as_secs_f64();

    println!(
        "Distance::Euclidean (SIMD):   {:.2} ms",
        simd_vector_time.as_secs_f64() * 1000.0
    );
    println!(
        "Distance::Euclidean (Scalar): {:.2} ms",
        scalar_vector_time.as_secs_f64() * 1000.0
    );
    println!("Speedup: {:.2}x", speedup);

    // Memory efficiency check
    println!("\n=== Memory Efficiency ===");
    println!("Float size: {} bytes", std::mem::size_of::<Float>());
    println!("Vector overhead: {} bytes", std::mem::size_of::<Vector>());
    println!(
        "1536-dim vector size: {} KB",
        (1536 * std::mem::size_of::<Float>()) as f64 / 1024.0
    );

    // SIMD width information
    println!("\n=== SIMD Configuration ===");
    #[cfg(target_arch = "aarch64")]
    {
        println!("Architecture: ARM64");
        println!("SIMD: NEON (128-bit, 4 floats per instruction)");
        println!("Theoretical max speedup: 4x");
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        println!("Architecture: x86_64");
        println!("SIMD: AVX2 (256-bit, 8 floats per instruction)");
        println!("Theoretical max speedup: 8x");
    }
    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "sse",
        not(target_feature = "avx2")
    ))]
    {
        println!("Architecture: x86_64");
        println!("SIMD: SSE (128-bit, 4 floats per instruction)");
        println!("Theoretical max speedup: 4x");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Run with: cargo test --ignored benchmark_performance -- --nocapture
    fn benchmark_performance() {
        run_benchmark();
    }
}
