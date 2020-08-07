use std::sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
};

use superposition::{
    dfs::{dfs, DfsError},
    futures::{
        hilberts_epsilon::{hilberts_epsilon, iproduct},
        sync::AsyncMutex,
        utils::yield_now,
        Controller, Executor, Simulator,
    },
};

#[test]
fn minimal_simulation() {
    struct MyTest;
    impl Controller for MyTest {
        fn on_restart(self, _: &Executor) -> Self {
            self
        }
        fn on_transition(self) -> Self {
            self
        }
        fn on_end_of_trajectory(self, _: &Executor) -> Self {
            self
        }
    }

    let sim = <Simulator<MyTest>>::new(MyTest);

    dfs(&sim, None).unwrap();
}

#[test]
fn simple_interleaving() {
    struct MyTest;
    impl Controller for MyTest {
        fn on_restart(self, ex: &Executor) -> Self {
            for _ in 0..2 {
                ex.spawn(async move {
                    for _ in 0..4 {
                        yield_now().await;
                    }
                })
                .detach();
            }
            self
        }
        fn on_transition(self) -> Self {
            self
        }
        fn on_end_of_trajectory(self, ex: &Executor) -> Self {
            assert_eq!(0, ex.unfinished_tasks());
            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest);

    dfs(&sim, None).unwrap();
}

#[test]
fn detects_race_condition() {
    #[derive(Default)]
    struct MyTest {
        a: Arc<AtomicIsize>,
        b: Arc<AtomicIsize>,
        num_failed: usize,
        num_trajectories: usize,
    };

    impl Controller for MyTest {
        #[inline]
        fn on_restart(self, ex: &Executor) -> Self {
            self.a.store(10, Ordering::SeqCst);
            self.b.store(10, Ordering::SeqCst);

            let lock: Arc<AsyncMutex<_>> = Arc::new(AsyncMutex::new(()));

            for i in 0usize..4 {
                let lock = lock.clone();
                let a = self.a.clone();
                let b = self.b.clone();
                ex.spawn(async move {
                    if i == 0 {
                        // Intentional bug: this lock is dropped too early, because the guard is
                        // not bound to anything.
                        let _ = lock.lock().await;

                        if a.load(Ordering::SeqCst) >= 10 {
                            yield_now().await;
                            a.fetch_sub(10, Ordering::SeqCst);
                            b.fetch_add(10, Ordering::SeqCst);
                        }
                    } else {
                        let _a = lock.lock().await;

                        if a.load(Ordering::SeqCst) >= 10 {
                            yield_now().await;
                            a.fetch_sub(10, Ordering::SeqCst);
                            b.fetch_add(10, Ordering::SeqCst);
                        }
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
        fn on_end_of_trajectory(mut self, ex: &Executor) -> Self {
            assert_eq!(0, ex.unfinished_tasks());

            let a_final = self.a.load(Ordering::SeqCst);
            let b_final = self.b.load(Ordering::SeqCst);

            if !(a_final == 0 && b_final == 20) {
                self.num_failed += 1;
            }

            self.num_trajectories += 1;

            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&sim, None).unwrap();

    let ret = sim.take_controller();
    assert!(ret.num_failed >= 1, "some trajectories should have failed");
    assert!(
        ret.num_failed < ret.num_trajectories,
        "some trajectorie should have succeeded"
    );
}

#[test]
fn detects_livelock() {
    #[derive(Default)]
    struct MyTest;

    impl Controller for MyTest {
        #[inline]
        fn on_restart(self, ex: &Executor) -> Self {
            ex.spawn(async move {
                loop {
                    yield_now().await;
                }
            })
            .detach();

            self
        }
        #[inline]
        fn on_transition(self) -> Self {
            self
        }
        #[inline]
        fn on_end_of_trajectory(self, _: &Executor) -> Self {
            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest::default());

    let ret = dfs(&sim, Some(100));

    assert_eq!(ret, Err(DfsError::MaxDepthExceeded(100)));
}

#[test]
fn detects_deadlock() {
    #[derive(Default)]
    struct MyTest;

    impl Controller for MyTest {
        #[inline]
        fn on_restart(self, ex: &Executor) -> Self {
            let a: Arc<AsyncMutex<()>> = Default::default();

            ex.spawn(async move {
                let _guard_0 = a.lock().await;
                let _guard_1 = a.lock().await;
            })
            .detach();

            self
        }
        #[inline]
        fn on_transition(self) -> Self {
            self
        }
        #[inline]
        fn on_end_of_trajectory(self, ex: &Executor) -> Self {
            assert_ne!(0, ex.unfinished_tasks());
            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&sim, Some(100)).unwrap();
}

#[test]
fn choice_operator_efficient_use() {
    #[derive(Default)]
    struct MyTest {
        tuples: Arc<std::sync::Mutex<std::collections::BTreeSet<(u8, i8, usize)>>>,
        num_trajectories: usize,
    };

    impl Controller for MyTest {
        #[inline]
        fn on_restart(self, ex: &Executor) -> Self {
            let tuples = self.tuples.clone();
            let ex_inner = ex.clone();
            ex.spawn(async move {
                let val =
                    hilberts_epsilon(ex_inner.clone(), iproduct!(0u8..=0, 0i8..=1, 0usize..=2))
                        .await;

                tuples.lock().unwrap().insert(val);
            })
            .detach();

            self
        }
        #[inline]
        fn on_transition(self) -> Self {
            self
        }
        #[inline]
        fn on_end_of_trajectory(mut self, ex: &Executor) -> Self {
            assert_eq!(0, ex.unfinished_tasks());

            self.num_trajectories += 1;

            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&sim, None).unwrap();

    let ret = sim.take_controller();
    let tuples: Vec<(_, _, _)> = ret.tuples.lock().unwrap().iter().copied().collect();
    assert_eq!(
        tuples,
        [
            (0, 0, 0),
            (0, 0, 1),
            (0, 0, 2),
            (0, 1, 0),
            (0, 1, 1),
            (0, 1, 2),
        ]
    );
    assert_eq!(6, tuples.len());
    assert_eq!(6, ret.num_trajectories);
}

#[test]
fn choice_operator_inefficient_use() {
    #[derive(Default)]
    struct MyTest {
        tuples: Arc<std::sync::Mutex<std::collections::BTreeSet<(u8, i8, usize)>>>,
        num_trajectories: usize,
    };

    impl Controller for MyTest {
        #[inline]
        fn on_restart(self, ex: &Executor) -> Self {
            let tuples = self.tuples.clone();
            let ex_inner = ex.clone();
            ex.spawn(async move {
                let a = hilberts_epsilon(ex_inner.clone(), 0u8..=0).await;
                let b = hilberts_epsilon(ex_inner.clone(), 0i8..=1).await;
                let c = hilberts_epsilon(ex_inner.clone(), 0usize..=2).await;

                let val = (a, b, c);

                tuples.lock().unwrap().insert(val);
            })
            .detach();

            self
        }
        #[inline]
        fn on_transition(self) -> Self {
            self
        }
        #[inline]
        fn on_end_of_trajectory(mut self, ex: &Executor) -> Self {
            assert_eq!(0, ex.unfinished_tasks());

            self.num_trajectories += 1;

            self
        }
    }
    let sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&sim, None).unwrap();

    let ret = sim.take_controller();
    let tuples: Vec<(_, _, _)> = ret.tuples.lock().unwrap().iter().copied().collect();
    assert_eq!(
        tuples,
        [
            (0, 0, 0),
            (0, 0, 1),
            (0, 0, 2),
            (0, 1, 0),
            (0, 1, 1),
            (0, 1, 2),
        ]
    );
    assert_eq!(6, tuples.len());
    assert!(ret.num_trajectories >= 50);
}

/// Tests that the trajectories in the system exactly match the expected counts.
///
/// The expected count is given by the multinomial coefficient, where the numerator is the
/// factorial of the product of the number of yields per process with the total number of
/// processes, and the denominator is the number of yields per process exponentiated by the number
/// of processes.
///
///    factorial(yields per process * processes) / (yields per process)**(processes)
///
/// Here is a table of values of the shape (A, B, C) where
/// A is the number of concurrent processes,
/// B is the number of explicit yield points + the implicit yield point when a task is spawned, and
/// C is the number of trajectories that will be run during the simulation. This is exactly equal
/// to the multinomial coefficient.
///
/// 0, 0+1, 1
/// 1, 0+1, 1
/// 1, 1+1, 1
/// 1, 2+1, 1
/// 1, 3+1, 1
/// 2, 1+1, 6
/// 2, 2+1, 20
/// 2, 3+1, 70
/// 2, 4+1, 252
/// 3, 0+1, 6
/// 3, 1+1, 90
/// 3, 2+1, 1680
/// 3, 3+1, 34650
#[test]
fn exact_combinatorics_all_trajectories_equals_multinomial_coefficient() {
    // Testing the Cartesian product of 0..=3 x 0..=3 takes too long unless this test is run with
    // optimizations on. So, take only some of the interesting cases and test those.
    let cases = vec![
        (0, 0),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4),
        (3, 0),
        (3, 1),
        (3, 2),
        // Uncomment this to recreate the table above:
        // (3, 3),
    ];
    for (n_processes, n_yields_explicit) in cases {
        #[derive(Copy, Clone)]
        struct MyTest {
            num_trajectories: u64,
            n_processes: u64,
            n_yields_explicit: u64,
        }
        impl Controller for MyTest {
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
            fn on_end_of_trajectory(mut self, ex: &Executor) -> Self {
                assert_eq!(0, ex.unfinished_tasks());
                self.num_trajectories += 1;
                self
            }
        }

        let sim = <Simulator<MyTest>>::new(MyTest {
            num_trajectories: 0,
            n_processes,
            n_yields_explicit,
        });

        dfs(&sim, None).unwrap();

        let ret = sim.take_controller();

        // The number of yields per task is always one greater than the explicit yields the user asks
        // for. The reason is that the task is not immediately run, but is instead queued to be
        // run; this is effectively a yield operation.
        let n_yields_all = n_yields_explicit + 1;

        // The expected number of trajectories is exactly equal to the multinomial coefficient.
        let want = multinomial_coefficient_brute(
            n_yields_all * n_processes,
            (0..n_processes).map(|_| n_yields_all),
        );
        assert!(want >= 1);

        assert_eq!(want, ret.num_trajectories);

        // Uncomment the following to recreate the table in the doc comment for this test.
        // println!("MC {}, {}+1, {}", n_processes, n_yields_explicit, ret.num_trajectories);
    }
}

/// Brute force. Easier than pulling in overly-big libraries to use a ln_gamma function.
fn multinomial_coefficient_brute(n: u64, mm: impl Iterator<Item = u64>) -> u64 {
    let mut top = factorial(n) as f64;
    for m in mm {
        top /= factorial(m) as f64;
    }
    top as u64
}

fn factorial(n: u64) -> u64 {
    if n == 0 {
        1
    } else {
        let mut acc = 1;
        for i in 1..=n {
            acc *= i;
        }
        acc
    }
}

#[test]
fn factorial_check() {
    assert_eq!(1, factorial(0));
    assert_eq!(1, factorial(1));
    assert_eq!(2, factorial(2));
    assert_eq!(6, factorial(3));
    assert_eq!(24, factorial(4));
}
