use crate::net::messages::Vector;
use crate::frontend::router::sharding::vector::Distance;
use std::time::Instant;

pub fn benchmark_distance() {
    println!("\n=== SIMD Distance Benchmark ===");
    
    // Test with OpenAI embedding size (1536 dimensions)
    let size = 1536;
    let iterations = 10000;
    
    // Create test vectors
    let v1: Vec<f32> = (0..size).map(|i| (i as f32) * 0.001).collect();
    let v2: Vec<f32> = (0..size).map(|i| (i as f32) * 0.001 + 0.5).collect();
    let v1 = Vector::from(v1);
    let v2 = Vector::from(v2);
    
    // Benchmark SIMD implementation
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Distance::Euclidean(&v1, &v2).distance();
    }
    let simd_time = start.elapsed();
    
    // Benchmark scalar implementation
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Distance::Euclidean(&v1, &v2).distance_scalar();
    }
    let scalar_time = start.elapsed();
    
    // Calculate speedup
    let speedup = scalar_time.as_secs_f64() / simd_time.as_secs_f64();
    
    println!("Vector size: {} dimensions", size);
    println!("Iterations: {}", iterations);
    println!("Scalar time: {:?}", scalar_time);
    println!("SIMD time: {:?}", simd_time);
    println!("Speedup: {:.2}x", speedup);
    
    // Test different vector sizes
    println!("\n=== Performance across different sizes ===");
    for size in [128, 256, 512, 768, 1024, 1536, 2048] {
        let v1: Vec<f32> = (0..size).map(|i| i as f32).collect();
        let v2: Vec<f32> = (0..size).map(|i| (i + 1) as f32).collect();
        let v1 = Vector::from(v1);
        let v2 = Vector::from(v2);
        
        let iterations = 10000;
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = Distance::Euclidean(&v1, &v2).distance();
        }
        let simd_time = start.elapsed();
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = Distance::Euclidean(&v1, &v2).distance_scalar();
        }
        let scalar_time = start.elapsed();
        
        let speedup = scalar_time.as_secs_f64() / simd_time.as_secs_f64();
        println!("Size {:4}: Speedup {:.2}x", size, speedup);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    #[ignore] // Run with: cargo test --ignored benchmark
    fn run_benchmark() {
        benchmark_distance();
    }
}