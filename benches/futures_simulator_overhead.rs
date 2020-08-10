#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use superposition::{
    dfs::Dfs,
    futures::{utils::yield_now, Controller, Executor, Simulator},
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
    fn on_restart(self, ex: &Executor) -> Self {
        for _ in 0..self.n_processes {
            ex.spawn(async move {
                for _ in 0..self.n_yields_explicit {
                    yield_now().await;
                }
            })
            .detach();
        }
        self
    }
    #[inline]
    fn on_transition(self) -> Self {
        self
    }
    #[inline]
    fn on_end_of_trajectory(self, _ex: &Executor) -> Self {
        //assert_eq!(0, ex.unfinished_tasks());
        self
    }
}

fn f(b: &mut criterion::Bencher, n_processes: usize, n_yields_explicit: usize) {
    b.iter_custom(move |iters| {
        let sim = <Simulator<MyBench>>::new(MyBench::new(n_processes, n_yields_explicit));

        let mut it = Dfs::new(&sim, None);

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
    let mut group = c.benchmark_group("futures-simulator-overhead");
    //group.bench_function("1p1y", move |b| f(b, 1, 1));
    //group.bench_function("1p2y", move |b| f(b, 1, 2));
    //group.bench_function("1p3y", move |b| f(b, 1, 3));
    //group.bench_function("1p4y", move |b| f(b, 1, 4));
    group.bench_function("2p1y", move |b| f(b, 2, 1));
    group.bench_function("2p2y", move |b| f(b, 2, 2));
    group.bench_function("2p3y", move |b| f(b, 2, 3));
    group.bench_function("2p4y", move |b| f(b, 2, 4));
    group.bench_function("2p45", move |b| f(b, 2, 5));
    group.bench_function("3p1y", move |b| f(b, 3, 1));
    group.bench_function("3p2y", move |b| f(b, 3, 2));
    group.bench_function("3p3y", move |b| f(b, 3, 3));
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
