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
    /// Every name the run can ask for, as pgdog mints them: `__pgdog_<counter>`.
    /// Index `i` holds `__pgdog_{i + 1}`.
    names: Vec<Key>,

    /// Every key in `0..n`, in random order.
    shuffled: Vec<usize>,

    /// `n` keys drawn uniformly from `0..n`.
    uniform: Vec<usize>,

    /// Zipf-distributed requests over `1..=n * 10`, where lower keys are hotter.
    /// The first `n` warm the cache [`Workload::cache`] builds,
    /// and the rest are the requests [`bench_traffic`] serves.
    zipf: Vec<usize>,
}

impl Workload {
    fn new(n: usize) -> Self {
        // Fixed seed so runs stay comparable.
        let mut rng = StdRng::seed_from_u64(0x5eed);

        let mut shuffled: Vec<usize> = (0..n).collect();
        shuffled.shuffle(&mut rng);

        let uniform = iter::repeat_with(|| rng.random_range(0..n))
            .take(n)
            .collect();

        // Ten times the cache, so most of the keyspace can't fit and misses keep coming.
        let distribution = Zipf::new((n * 10) as f64, 1.0).expect("zipf parameters are valid");
        let zipf = iter::repeat_with(|| rng.sample(distribution) as usize - 1)
            // `n` to warm a cache with, then ten passes for bench_traffic to serve.
            // Rebuilding the cache in setup costs about twice one pass over it, so the
            // measured region has to be several passes long to keep that out of the numbers.
            .take(n * 11)
            .collect();

        // Built once so the measured loops only ever clone a name, never format one.
        let names = (1..=n * 10).map(|c| format!("__pgdog_{c}")).collect();

        Self {
            names,
            shuffled,
            uniform,
            zipf,
        }
    }

    fn entries(&self) -> usize {
        self.shuffled.len()
    }

    /// The name at `index`.
    fn name(&self, index: usize) -> &Key {
        &self.names[index]
    }

    /// Returns a full cache, warmed with Zipf requests so LFU entries don't all tie on use count.
    fn cache<C: Cache>(&self) -> C {
        let n = self.entries();
        let mut cache = C::new(n);

        for index in 0..n {
            cache.insert(self.name(index).clone(), Value::new());
        }

        for &index in &self.zipf[..n] {
            cache.get(self.name(index));
        }

        cache
    }

    /// The requests [`bench_traffic`] serves, drawn after the ones [`Workload::cache`] warms with.
    fn traffic(&self) -> &[usize] {
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
                    for &index in &workload.shuffled {
                        cache.insert(workload.name(index).clone(), Value::new());
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
                    for &index in &workload.uniform {
                        black_box(cache.get(workload.name(index)));
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
                for &index in &workload.uniform {
                    black_box(cache.peek(workload.name(index)));
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
                    for &index in &workload.shuffled {
                        black_box(cache.remove(workload.name(index)));
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
                    for &index in requests {
                        let key = workload.name(index);

                        if cache.get(key).is_none() {
                            cache.insert(key.clone(), Value::new());
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
