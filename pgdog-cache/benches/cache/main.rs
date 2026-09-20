mod adapters;

use std::{hint::black_box, iter};

use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use rand_distr::Zipf;

use adapters::*;

type Group<'a> = BenchmarkGroup<'a, WallTime>;

struct Workload {
    /// Every key in `0..n`, in random order.
    shuffled: Vec<Key>,

    /// `n` keys drawn uniformly from `0..n`.
    uniform: Vec<Key>,

    /// Zipf-distributed requests over `1..=n * 10`, where lower keys are hotter.
    /// The first `n` warm the cache [`Workload::cache`] builds,
    /// and the rest are the requests [`bench_traffic`] serves.
    zipf: Vec<Key>,
}

impl Workload {
    fn new(n: usize) -> Self {
        // Fixed seed so runs stay comparable.
        let mut rng = StdRng::seed_from_u64(0x5eed);

        let mut shuffled: Vec<Key> = (0..n as Key).collect();
        shuffled.shuffle(&mut rng);

        let uniform = iter::repeat_with(|| rng.random_range(0..n as Key))
            .take(n)
            .collect();

        // Ten times the cache, so most of the keyspace can't fit and misses keep coming.
        let distribution = Zipf::new((n * 10) as f64, 1.0).expect("zipf parameters are valid");
        let zipf = iter::repeat_with(|| rng.sample(distribution) as Key)
            // `n` to warm a cache with, then ten passes for bench_traffic to serve.
            // Rebuilding the cache in setup costs about twice one pass over it, so the
            // measured region has to be several passes long to keep that out of the numbers.
            .take(n * 11)
            .collect();

        Self {
            shuffled,
            uniform,
            zipf,
        }
    }

    fn entries(&self) -> usize {
        self.shuffled.len()
    }

    /// Returns a full cache, warmed with Zipf requests so LFU entries don't all tie on use count.
    fn cache<C: Cache>(&self) -> C {
        let n = self.entries();
        let mut cache = C::new(n);

        for key in 0..n as Key {
            cache.insert(key, key);
        }

        for key in &self.zipf[..n] {
            cache.get(key);
        }

        cache
    }

    /// The requests [`bench_traffic`] serves, drawn after the ones [`Workload::cache`] warms with.
    fn traffic(&self) -> &[Key] {
        &self.zipf[self.entries()..]
    }
}

/// Runs `benches` over both cache sizes, in one criterion group named `name`.
fn compare(c: &mut Criterion, name: &str, benches: &[fn(&mut Group<'_>, &Workload)]) {
    let mut group = c.benchmark_group(name);

    for n in [64, 1024] {
        let workload = Workload::new(n);

        group.throughput(Throughput::Elements(n as u64));

        for bench in benches {
            bench(&mut group, &workload);
        }
    }

    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    fn bench<C: Cache>(group: &mut Group<'_>, workload: &Workload) {
        group.bench_function(BenchmarkId::new(C::NAME, workload.entries()), |b| {
            b.iter_batched_ref(
                || C::new(workload.entries()),
                |cache| {
                    for &key in &workload.shuffled {
                        cache.insert(key, key);
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    compare(
        c,
        "insert",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

fn bench_get(c: &mut Criterion) {
    fn bench<C: Cache>(group: &mut Group<'_>, workload: &Workload) {
        group.bench_function(BenchmarkId::new(C::NAME, workload.entries()), |b| {
            b.iter_batched_ref(
                || workload.cache::<C>(),
                |cache| {
                    for key in &workload.uniform {
                        black_box(cache.get(key));
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    compare(
        c,
        "get",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

fn bench_peek(c: &mut Criterion) {
    fn bench<C: Peek>(group: &mut Group<'_>, workload: &Workload) {
        let cache = workload.cache::<C>();

        group.bench_function(BenchmarkId::new(C::NAME, workload.entries()), |b| {
            b.iter(|| {
                for key in &workload.uniform {
                    black_box(cache.peek(key));
                }
            });
        });
    }

    compare(
        c,
        "peek",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

fn bench_remove(c: &mut Criterion) {
    fn bench<C: Remove>(group: &mut Group<'_>, workload: &Workload) {
        group.bench_function(BenchmarkId::new(C::NAME, workload.entries()), |b| {
            b.iter_batched_ref(
                || workload.cache::<C>(),
                |cache| {
                    for key in &workload.shuffled {
                        black_box(cache.remove(key));
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    compare(
        c,
        "remove",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

fn bench_pop(c: &mut Criterion) {
    fn bench<C: Pop>(group: &mut Group<'_>, workload: &Workload) {
        group.bench_function(BenchmarkId::new(C::NAME, workload.entries()), |b| {
            b.iter_batched_ref(
                || workload.cache::<C>(),
                |cache| {
                    while let Some(entry) = cache.pop() {
                        black_box(entry);
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    compare(
        c,
        "pop",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

/// Serves Zipf-distributed requests through a cache of `n` entries.
/// A miss inserts and then evicts down to `n`, the order pgdog's prepared statement cache uses.
/// Under LFU that order can evict the new key, which cachekit and evictor never do.
fn bench_traffic(c: &mut Criterion) {
    fn bench<C: Cache>(group: &mut Group<'_>, workload: &Workload) {
        let n = workload.entries();
        let requests = workload.traffic();

        // This group serves ten requests per entry, not the one pass every other
        // group does, so it counts its own elements.
        group.throughput(Throughput::Elements(requests.len() as u64));
        group.bench_function(BenchmarkId::new(C::NAME, n), |b| {
            b.iter_batched_ref(
                || workload.cache::<C>(),
                |cache| {
                    for &key in requests {
                        if cache.get(&key).is_none() {
                            cache.insert(key, key);
                            cache.evict_to(n);
                        }
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    compare(
        c,
        "traffic",
        &[
            bench::<PgdogLru>,
            bench::<Lru>,
            bench::<Schnellru>,
            bench::<CachekitLru>,
            bench::<EvictorLru>,
            bench::<PgdogLfu>,
            bench::<CachekitLfu>,
            bench::<EvictorLfu>,
        ],
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().noise_threshold(0.10);
    targets = bench_insert, bench_get, bench_peek, bench_remove, bench_pop, bench_traffic
}

criterion_main!(benches);
