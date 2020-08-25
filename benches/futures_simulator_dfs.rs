#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use superposition::{
    dfs::Dfs,
    futures::{utils::yield_now, Controller, Executor, Simulator, Spawner},
};

#[derive(Default, Copy, Clone)]
struct MyBench {
    n_processes: usize,
    n_yields_explicit: usize,
}

impl MyBench {
    fn new(n_processes: usize, n_yields_explicit: usize) -> Self {
        Self {
            n_processes,
            n_yields_explicit,
        }
    }
}

impl Controller for MyBench {
    #[inline]
    fn on_restart(&mut self, spawner: &Spawner) {
        for _ in 0..self.n_processes {
            spawner.spawn_detach({
                let y = self.n_yields_explicit;
                async move {
                    for _ in 0..y {
                        yield_now().await;
                    }
                }
            });
        }
    }
    #[inline]
    fn on_transition(&mut self) {}
    #[inline]
    fn on_end_of_trajectory(&mut self, _ex: &Executor) {}
}

// TODO(rw): Reuse DFS state to minimize spurious allocations.
fn f(b: &mut criterion::Bencher, n_processes: usize, n_yields_explicit: usize) {
    b.iter_custom(move |iters| {
        let mut sim = Simulator::new(MyBench::new(n_processes, n_yields_explicit));

        let mut it = Dfs::new(&mut sim, None);

        let start = Instant::now();
        for _ in 0..iters {
            match it.next() {
                Some(Ok(_)) => (),
                None => it.restart(),
                Some(Err(_)) => unreachable!(),
            }
        }
        start.elapsed()
    });
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("futures-simulator-dfs");
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
