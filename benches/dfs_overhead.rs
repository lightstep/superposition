#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use criterion::{criterion_group, criterion_main, Criterion};

use superposition::{dfs::Dfs, kripke_structure::KripkeStructure};

#[derive(Default, Clone)]
struct MyBench {
    depth: Arc<AtomicUsize>,

    max_depth: usize,
    breadth: usize,
}

impl MyBench {
    fn new(max_depth: usize, breadth: usize) -> Self {
        Self {
            depth: Arc::new(AtomicUsize::new(0)),
            max_depth,
            breadth,
        }
    }
}

impl KripkeStructure for &MyBench {
    type Label = usize;
    type LabelIterator = std::ops::Range<usize>;

    #[inline]
    fn transition(self, _: Self::Label) {
        self.depth.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    fn successors(self) -> Option<Self::LabelIterator> {
        if self.depth.load(Ordering::SeqCst) < self.max_depth {
            Some(0..self.breadth)
        } else {
            None
        }
    }

    #[inline]
    fn restart(self) {
        self.depth.store(0, Ordering::SeqCst);
    }
}

fn f(b: &mut criterion::Bencher, max_depth: usize, breadth: usize) {
    b.iter_custom(move |iters| {
        let sim = MyBench::new(max_depth, breadth);

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
    let mut group = c.benchmark_group("dfs-overhead");
    group.bench_function("1d1b", move |b| f(b, 1, 1));
    group.bench_function("2d1b", move |b| f(b, 2, 1));
    group.bench_function("2d2b", move |b| f(b, 2, 2));
    group.bench_function("2d3b", move |b| f(b, 2, 3));
    group.bench_function("2d4b", move |b| f(b, 2, 4));
    group.bench_function("2d5b", move |b| f(b, 2, 5));
    group.bench_function("3d1b", move |b| f(b, 3, 1));
    group.bench_function("3d2b", move |b| f(b, 3, 2));
    group.bench_function("3d3b", move |b| f(b, 3, 3));
    group.bench_function("4d4b", move |b| f(b, 4, 4));
    group.bench_function("5d5b", move |b| f(b, 5, 5));
    group.bench_function("6d6b", move |b| f(b, 6, 6));
    group.bench_function("7d7b", move |b| f(b, 7, 7));
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
