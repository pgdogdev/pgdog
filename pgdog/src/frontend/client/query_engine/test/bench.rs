//! Micro benchmarking the QueryEngine

#[cfg(feature = "bench")]
use divan::AllocProfiler;

/// Profile memory usage in bench
#[cfg(feature = "bench")]
#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

/// Creates a benchmark that runs the extracted inner-test.
/// Allows code-reuse between tests and benches (with a simple extract + macro addition)
///
/// Unfortunately, right now, there's some extra overhead from creating a Client, QueryEngine every single iteration...
/// this is a necessity.
macro_rules! divan_tokio_bench {
    ($name:ident) => {
        paste::paste! {
            // TODO: I think this should also be minimum runtime of 1s to reduce variance
            #[divan::bench(sample_count = 1000)]
            fn [< $name _bench >](bencher: divan::Bencher) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                bencher.bench_local(|| rt.block_on($name()));
            }
        }
    };
}

#[test]
#[cfg(feature = "bench")]
fn run_benches() {
    // Starts off the benchmarks within other files (identified by #[divan::bench] / the macro)
    divan::Divan::default().run_benches();
}
