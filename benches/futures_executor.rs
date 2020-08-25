#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use superposition::futures::{utils::yield_now, Executor};

fn f(b: &mut criterion::Bencher, n_spawns: usize, n_yields_explicit: usize) {
    b.iter_custom(move |iters| {
        let mut ex = Executor::default();
        let spawner = ex.spawner();

        let start = Instant::now();

        for _ in 0..iters {
            for _ in 0..n_spawns {
                spawner.spawn_detach(async move {
                    for _ in 0..n_yields_explicit {
                        yield_now().await;
                    }
                });
            }

            ex.reset();
        }

        start.elapsed()
    });
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("futures-executor-spawn-reset-heap-dyn");
    group.bench_function("1p1y", move |b| f(b, 1, 1));
    group.bench_function("2p1y", move |b| f(b, 2, 1));
    group.bench_function("2p2y", move |b| f(b, 2, 2));
    group.bench_function("2p3y", move |b| f(b, 2, 3));
    group.bench_function("2p4y", move |b| f(b, 2, 4));
    group.bench_function("2p5y", move |b| f(b, 2, 5));
    group.bench_function("3p1y", move |b| f(b, 3, 1));
    group.bench_function("3p2y", move |b| f(b, 3, 2));
    group.bench_function("3p3y", move |b| f(b, 3, 3));
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
