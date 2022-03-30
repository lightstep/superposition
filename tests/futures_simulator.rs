use std::{cell::RefCell, rc::Rc};

use superposition::{
    dfs::{dfs, Dfs, DfsError},
    futures::{
        sync::IntrusiveAsyncMutex, utils::yield_now, ChoiceSet, ChoiceSetRecvResult, ChoiceStream,
        ChoiceTaken, Controller, Executor, HilbertsEpsilonId, Simulator, Spawner, TaskId,
    },
};

#[test]
fn minimal_simulation() {
    struct MyTest;
    impl Controller for MyTest {
        fn on_restart(&mut self, _: &Spawner) {}
        fn on_transition(&mut self, _: ChoiceTaken) {}
        fn on_end_of_trajectory(&mut self, _: &Executor) {}
    }

    let mut sim = Simulator::new(MyTest);

    Dfs::new(&mut sim, None).run_to_completion().unwrap();
}

#[test]
fn simple_interleaving() {
    struct MyTest;
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            for _ in 0..2 {
                spawner.spawn_detach(async move {
                    for _ in 0..4 {
                        yield_now().await;
                    }
                });
            }
        }
        fn on_transition(&mut self, _: ChoiceTaken) {}
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest);

    dfs(&mut sim, None).unwrap();
}

use futures_util::stream;
use futures_util::stream::StreamExt;

#[test]
fn stream_iteration() {
    #[derive(Default)]
    struct MyTest {
        num_trajectories: usize,
        values: Rc<RefCell<Vec<u8>>>,
    }
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            let mut s = stream::iter(vec![1, 2, 3]);
            let values = self.values.clone();
            spawner.spawn_detach(async move {
                while let Some(v) = s.next().await {
                    yield_now().await;
                    values.borrow_mut().push(v);
                }
            });
        }
        fn on_transition(&mut self, _: ChoiceTaken) {}
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            self.num_trajectories += 1;
            assert_eq!(0, ex.unfinished_tasks());
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());
    dfs(&mut sim, None).unwrap();
    let ret = sim.take_controller();

    let values: Vec<u8> = ret.values.borrow().iter().copied().collect();
    #[rustfmt::skip]
    assert_eq!(values, [
        1,
        1, 2,
        1, 2, 3
    ]);

    assert!(ret.num_trajectories == 1);
}

#[test]
fn detects_race_condition() {
    #[derive(Default)]
    struct MyTest {
        a: Rc<RefCell<isize>>,
        b: Rc<RefCell<isize>>,
        num_failed: usize,
        num_trajectories: usize,
    }

    impl Controller for MyTest {
        #[inline]
        fn on_restart(&mut self, spawner: &Spawner) {
            *self.a.borrow_mut() = 10;
            *self.b.borrow_mut() = 10;

            let lock: Rc<IntrusiveAsyncMutex<_>> = Rc::new(IntrusiveAsyncMutex::new((), false));

            for i in 0usize..4 {
                let lock = lock.clone();
                let a = self.a.clone();
                let b = self.b.clone();
                spawner.spawn_detach(async move {
                    if i == 0 {
                        // Intentional bug: this lock is dropped too early, because the guard is
                        // not bound to anything.
                        #[allow(clippy::let_underscore_lock)]
                        let _ = lock.lock().await;

                        if *a.borrow() >= 10 {
                            yield_now().await;
                            *a.borrow_mut() -= 10;
                            *b.borrow_mut() += 10;
                        }
                    } else {
                        let _a = lock.lock().await;

                        if *a.borrow() >= 10 {
                            yield_now().await;
                            *a.borrow_mut() -= 10;
                            *b.borrow_mut() += 10;
                        }
                    }
                });
            }
        }
        #[inline]
        fn on_transition(&mut self, _: ChoiceTaken) {}
        #[inline]
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            let a_final = *self.a.borrow();
            let b_final = *self.b.borrow();

            if !(a_final == 0 && b_final == 20) {
                self.num_failed += 1;
            }

            self.num_trajectories += 1;
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&mut sim, None).unwrap();

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
        fn on_restart(&mut self, spawner: &Spawner) {
            spawner.spawn_detach(async move {
                loop {
                    yield_now().await;
                }
            });
        }
        #[inline]
        fn on_transition(&mut self, _: ChoiceTaken) {}
        #[inline]
        fn on_end_of_trajectory(&mut self, _: &Executor) {}
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());

    let ret = dfs(&mut sim, Some(100));

    assert_eq!(ret, Err(DfsError::MaxDepthExceeded(101)));
}

#[test]
fn detects_deadlock() {
    #[derive(Default)]
    struct MyTest {
        num_trajectories_with_unfinished_tasks: usize,
    }

    impl Controller for MyTest {
        #[inline]
        fn on_restart(&mut self, spawner: &Spawner) {
            let a: Rc<IntrusiveAsyncMutex<()>> = Rc::new(IntrusiveAsyncMutex::new((), false));
            let b: Rc<IntrusiveAsyncMutex<()>> = Rc::new(IntrusiveAsyncMutex::new((), false));

            spawner.spawn_detach({
                let a = a.clone();
                let b = b.clone();

                async move {
                    let _guard_a = a.lock().await;

                    // TODO(rw): write our own sync primitives that always correctly yield to the
                    // scheduler.
                    yield_now().await;

                    let _guard_b = b.lock().await;
                }
            });

            spawner.spawn_detach(async move {
                let _guard_b = b.lock().await;
                let _guard_a = a.lock().await;
            });
        }
        #[inline]
        fn on_transition(&mut self, _: ChoiceTaken) {}
        #[inline]
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            if ex.unfinished_tasks() >= 1 {
                self.num_trajectories_with_unfinished_tasks += 1;
            }
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&mut sim, Some(100)).unwrap();

    let c = sim.take_controller();

    assert!(c.num_trajectories_with_unfinished_tasks >= 1);
}

#[test]
fn multiple_hilberts_epsilons_do_not_explode_state_space() {
    #[derive(Default)]
    struct MyTest {
        tuples: Rc<RefCell<std::collections::BTreeSet<(u8, i8, usize)>>>,
        num_trajectories: usize,
    }

    impl Controller for MyTest {
        #[inline]
        fn on_restart(&mut self, spawner: &Spawner) {
            let tuples = self.tuples.clone();
            let spawner_inner = spawner.clone();
            spawner.spawn_detach(async move {
                let a = spawner_inner.hilberts_epsilon(1).await as u8;
                let b = spawner_inner.hilberts_epsilon(2).await as i8;
                let c = spawner_inner.hilberts_epsilon(3).await as usize;

                tuples.borrow_mut().insert((a, b, c));
            });
        }

        #[inline]
        fn on_transition(&mut self, _: ChoiceTaken) {}

        #[inline]
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            self.num_trajectories += 1;
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&mut sim, None).unwrap();

    let ret = sim.take_controller();
    let tuples: Vec<(_, _, _)> = ret.tuples.borrow().iter().copied().collect();
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
fn nested_hilberts_epsilons_increase_state_space_only_minimally() {
    #[derive(Default)]
    struct MyTest {
        num_trajectories: usize,
    }

    impl Controller for MyTest {
        #[inline]
        fn on_restart(&mut self, spawner: &Spawner) {
            spawner.spawn_detach({
                let s = spawner.clone();
                async move {
                    if s.hilberts_epsilon(2).await == 0 {
                        match s.hilberts_epsilon(3).await {
                            // NOTE(rw): These calls all depend on one configuration of the parent
                            // hilberts_epsilon calls, so they create only 5+7+11 universes.
                            // This is analogous to an enum, or "sum" algebraic data types in
                            // programming languages.
                            0 => s.hilberts_epsilon(5).await,
                            1 => s.hilberts_epsilon(7).await,
                            2 => s.hilberts_epsilon(11).await,
                            _ => unreachable!(),
                        };
                    } else {
                        // NOTE(rw): These calls are independent of each other, and dependent on
                        // their one parent universe, so this creates 13*17 universes.
                        // This is analogous to a tuple, or "product" algebraic data types in
                        // programming languages.
                        s.hilberts_epsilon(13).await;
                        s.hilberts_epsilon(17).await;
                    }
                }
            });
        }

        #[inline]
        fn on_transition(&mut self, _: ChoiceTaken) {}

        #[inline]
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            self.num_trajectories += 1;
        }
    }
    let mut sim = <Simulator<MyTest>>::new(MyTest::default());

    dfs(&mut sim, None).unwrap();

    let ret = sim.take_controller();
    assert_eq!(5 + 7 + 11 + 13 * 17, ret.num_trajectories);
}

#[test]
fn choice_stream_validity() {
    #[derive(Default)]
    struct TestState {
        seen_choices: Vec<usize>,
        num_trajectories: usize,

        /// The choice made by the current trajectory, collected at the end.
        this_choice: Rc<RefCell<usize>>,
    }

    impl Controller for TestState {
        fn on_restart(&mut self, spawner: &Spawner) {
            let mut stream = ChoiceStream::new(spawner, 0..5); // Choose 0 through 4.
            let this_choice = self.this_choice.clone();

            // Enqueue a task that reads a single value from the ChoiceStream.
            spawner.spawn_detach(async move {
                let choice: usize = stream.next().await.unwrap();
                *this_choice.borrow_mut() = choice;

                // The stream should yield only a single choice.
                assert_eq!(stream.next().await, None)
            });
        }

        fn on_transition(&mut self, _: ChoiceTaken) {}

        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            // Count the choice at the trajectory's end.
            let this_choice = *self.this_choice.borrow();
            self.seen_choices.push(this_choice);

            self.num_trajectories += 1;
        }
    }

    let mut sim = Simulator::new(TestState::default());
    dfs(&mut sim, None).unwrap();

    let state: TestState = sim.take_controller();
    let mut choices: Vec<usize> = state.seen_choices.to_vec();
    choices.sort_unstable();
    assert_eq!(choices, vec![0, 1, 2, 3, 4]);
    assert_eq!(state.num_trajectories, 5);
}

#[test]
fn task_id_tracking() {
    use std::collections::HashMap;

    #[derive(Default)]
    struct MyTest {
        log: Vec<&'static str>,
        logs: Vec<Vec<&'static str>>,
        names: HashMap<TaskId, &'static str>,
    }
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            {
                let task_id = spawner.spawn_detach(async move {});
                self.names.insert(task_id, "a");
            }

            {
                let task_id = spawner.spawn_detach(async move {});
                self.names.insert(task_id, "b");
            }
        }
        fn on_transition(&mut self, choice_taken: ChoiceTaken) {
            match choice_taken {
                ChoiceTaken::TaskPollPending { id } | ChoiceTaken::TaskPollReady { id } => {
                    let name = self.names.get(&id).expect("unknown task id, logic error");
                    self.log.push(name);
                }
                ChoiceTaken::HilbertsEpsilon { .. } | ChoiceTaken::TaskNoLongerExists { .. } => {
                    panic!("should not happen")
                }
            }
        }
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            let log = std::mem::take(&mut self.log);
            self.logs.push(log);
        }
    }
    let mut sim: Simulator<MyTest> = Default::default();

    dfs(&mut sim, None).unwrap();

    let mut logs = sim.take_controller().logs;
    logs.sort();

    // TODO(rw): prevent the extraneous first task choice that is happening here.
    #[rustfmt::skip]
    assert_eq!(vec![
        vec!["a", "a", "b"],
        vec!["b", "b", "a"],
    ], logs);
}

#[test]
fn hilbert_epsilon_id_tracking() {
    use std::collections::HashMap;

    #[derive(Default)]
    struct MyTest {
        log: Vec<(&'static str, usize)>,
        logs: Vec<Vec<(&'static str, usize)>>,
        names: Rc<RefCell<HashMap<HilbertsEpsilonId, &'static str>>>,
    }
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            spawner.spawn_detach({
                let spawner2 = spawner.clone();
                let names = self.names.clone();
                async move {
                    for name in &["a", "b"] {
                        let he_fut = spawner2.hilberts_epsilon(2);

                        names.clone().borrow_mut().insert(he_fut.id(), name);
                    }
                }
            });
        }
        fn on_transition(&mut self, choice_taken: ChoiceTaken) {
            match choice_taken {
                ChoiceTaken::TaskPollPending { .. } | ChoiceTaken::TaskPollReady { .. } => (),
                ChoiceTaken::HilbertsEpsilon { id, universe } => {
                    let name: &'static str = {
                        let names = self.names.borrow();
                        names.get(&id).expect("unknown task id, logic error")
                    };
                    self.log.push((name, universe));
                }
                ChoiceTaken::TaskNoLongerExists { .. } => panic!("should not happen"),
            }
        }
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            let log = std::mem::take(&mut self.log);
            self.logs.push(log);
        }
    }
    let mut sim: Simulator<MyTest> = Default::default();

    dfs(&mut sim, None).unwrap();

    let mut logs = sim.take_controller().logs;
    logs.sort();

    // TODO(rw): prevent the extraneous first hilberts epsilon choice that is happening here.
    #[rustfmt::skip]
    assert_eq!(
        vec![
            vec![("a", 0), ("a", 0), ("b", 0)],
            vec![("a", 0), ("b", 1)],
            vec![("a", 1), ("a", 1), ("b", 0)],
            vec![("a", 1), ("b", 1)],
        ],
        logs
    );
}

#[test]
fn choice_set_validity() {
    #[derive(PartialOrd, PartialEq, Eq, Ord, Debug)]
    enum E {
        Out(u8),
        In(ChoiceSetRecvResult<u8>),
    }
    #[derive(Default)]
    struct MyTest {
        set: ChoiceSet<u8>,
        log: Rc<RefCell<Vec<E>>>,
        logs: Rc<RefCell<Vec<Vec<E>>>>,
    }
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            self.set.reset();
            self.log.borrow_mut().clear();

            spawner.spawn_detach({
                let set = self.set.clone();
                let log = self.log.clone();
                let spawner2 = spawner.clone();

                async move {
                    // Send a value.
                    set.send(0).await;
                    log.borrow_mut().push(E::Out(0));

                    // Try to receive a value once.
                    let got = set.recv(&spawner2).await;
                    log.borrow_mut().push(E::In(got));

                    // Try to receive a value again.
                    let got = set.recv(&spawner2).await;
                    log.borrow_mut().push(E::In(got));
                }
            });
        }
        fn on_transition(&mut self, _: ChoiceTaken) {}
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            let log = self.log.replace(Vec::new());
            self.logs.borrow_mut().push(log);
        }
    }
    let mut sim: Simulator<MyTest> = Default::default();

    dfs(&mut sim, None).unwrap();

    let mut got = sim.take_controller().logs.replace(Vec::new());

    let want = vec![
        // Value sent then lost (e.g. lost in transit).
        vec![
            E::Out(0),
            E::In(ChoiceSetRecvResult::Lost(0)),
            E::In(ChoiceSetRecvResult::Empty),
        ],
        // Value sent then received.
        vec![
            E::Out(0),
            E::In(ChoiceSetRecvResult::Received(0)),
            E::In(ChoiceSetRecvResult::Empty),
        ],
    ];

    got.sort();

    assert_eq!(want, got);
}

#[test]
fn choice_set_client_server() {
    /// An event to log for testing.
    #[derive(PartialOrd, PartialEq, Eq, Ord, Debug)]
    enum E {
        Out(u8),
        In(ChoiceSetRecvResult<u8>),
    }
    #[derive(Default)]
    struct MyTest {
        set: ChoiceSet<u8>,
        log: Rc<RefCell<Vec<E>>>,
        logs: Rc<RefCell<Vec<Vec<E>>>>,
    }
    impl Controller for MyTest {
        fn on_restart(&mut self, spawner: &Spawner) {
            self.set.reset();
            self.log.borrow_mut().clear();

            // Spawn a sender, and send values unconditionally.
            spawner.spawn_detach({
                let set = self.set.clone();
                let log = self.log.clone();

                async move {
                    for i in 0u8..2 {
                        set.send(i).await;
                        log.borrow_mut().push(E::Out(i));
                    }
                }
            });

            // Spawn a receiver, and recieve messages until the buffer indicates it is empty.
            spawner.spawn_detach({
                let spawner2 = spawner.clone();
                let set = self.set.clone();
                let log = self.log.clone();

                async move {
                    loop {
                        let got = set.recv(&spawner2).await;
                        log.borrow_mut().push(E::In(got));
                        if let ChoiceSetRecvResult::Empty = got {
                            break;
                        }
                    }
                }
            });
        }
        fn on_transition(&mut self, _: ChoiceTaken) {}
        fn on_end_of_trajectory(&mut self, ex: &Executor) {
            assert_eq!(0, ex.unfinished_tasks());

            let log = self.log.replace(Vec::new());
            self.logs.borrow_mut().push(log);
        }
    }

    // Run the sim and extract the observed logs.
    let mut sim: Simulator<MyTest> = Default::default();
    dfs(&mut sim, None).unwrap();
    let mut got = sim.take_controller().logs.replace(Vec::new());

    let want = {
        let mut want = vec![];
        want.extend(vec![
            // First value: sent then lost (e.g. lost in transit).
            // Second value: ditto.
            // Ordering 1 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then lost (e.g. lost in transit).
            // Second value: ditto.
            // Ordering 2 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then lost (e.g. lost in transit).
            // Second value: ditto.
            // Ordering 3 of 3.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
        ]);
        want.extend(vec![
            // First value: sent then received.
            // Second value: ditto.
            // Ordered 1 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then received.
            // Second value: ditto.
            // Ordered 2 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then received.
            // Second value: ditto.
            // Ordering 3 of 3.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
        ]);
        want.extend(vec![
            // First value: sent then received.
            // Second value: sent then lost.
            // Ordering 1 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then received.
            // Second value: sent then lost.
            // Ordering 2 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then received.
            // Second value: sent then lost.
            // Ordering 3 of 3.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
        ]);
        want.extend(vec![
            // First value: sent then lost.
            // Second value: sent then received.
            // Ordering 1 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then lost.
            // Second value: sent then received.
            // Ordering 2 of 3.
            vec![
                E::Out(0),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
            // First value: sent then lost.
            // Second value: sent then received.
            // Ordering 3 of 3.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::Out(1),
                E::In(ChoiceSetRecvResult::Received(1)),
                E::In(ChoiceSetRecvResult::Empty),
            ],
        ]);
        want.extend(vec![
            // First value: sent then lost.
            // Second value: sent after receiver terminates.
            // Ordering 1 of 1.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Lost(0)),
                E::In(ChoiceSetRecvResult::Empty),
                E::Out(1),
            ],
            // First value: sent after receiver terminates.
            // Second value: sent after receiver terminates.
            // Ordering 1 of 1.
            vec![E::In(ChoiceSetRecvResult::Empty), E::Out(0), E::Out(1)],
            // First value: sent then received.
            // Second value: sent after receiver terminates.
            // Ordering 1 of 1.
            vec![
                E::Out(0),
                E::In(ChoiceSetRecvResult::Received(0)),
                E::In(ChoiceSetRecvResult::Empty),
                E::Out(1),
            ],
        ]);
        want.sort();
        want
    };

    // Assert the sizes of the un-dedupped wanted and observed logs.
    // TODO(rw): Figure out how to lower the number of duplicated trajectories: ideally, the number
    // of observed logs should be equal to the number of unique trajectories.
    assert_eq!(want.len(), 15);
    assert_eq!(got.len(), 49);

    // Deduplicate the observed logs.
    got.sort();
    got.dedup();

    assert_eq!(want, got);
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
            fn on_transition(&mut self, _: ChoiceTaken) {}
            #[inline]
            fn on_end_of_trajectory(&mut self, ex: &Executor) {
                assert_eq!(0, ex.unfinished_tasks());
                self.num_trajectories += 1;
            }
        }

        let mut sim = <Simulator<MyTest>>::new(MyTest {
            num_trajectories: 0,
            n_processes,
            n_yields_explicit,
        });

        dfs(&mut sim, None).unwrap();

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
