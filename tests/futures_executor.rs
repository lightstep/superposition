use std::cell::RefCell;
use std::rc::Rc;

use superposition::futures::{sync::AsyncMutex, utils::yield_now, Executor};

#[test]
fn drives_concurrent_locks_to_completion() {
    let mut ex = Executor::default();
    let spawner = ex.spawner();

    let counter = Rc::new(RefCell::new(0_isize));

    spawner.spawn_detach({
        let lock = Rc::new(AsyncMutex::new(()));

        let spawner = spawner.clone();
        let counter = counter.clone();

        async move {
            for _ in 0..1000 {
                let counter = counter.clone();
                let lock = lock.clone();
                spawner.spawn_detach({
                    async move {
                        yield_now().await;
                        {
                            let _z = lock.lock().await;
                        }
                        yield_now().await;
                        {
                            let _z = lock.lock().await;
                        }
                        yield_now().await;
                        *counter.borrow_mut() += 1;
                    }
                });
            }
        }
    });

    while ex.choose_any() {}

    assert_eq!(0, ex.choices());
    assert_eq!(0, ex.unfinished_tasks());

    let got = *counter.borrow();
    assert_eq!(1000, got);
}

#[test]
fn detects_deadlock() {
    let mut ex = Executor::default();
    let spawner = ex.spawner();
    spawner.spawn_detach({
        let m = Rc::new(AsyncMutex::new(()));
        async move {
            let _a = m.lock().await;
            let _b = m.lock().await;
        }
    });

    while ex.choose_any() {}
    assert!(
        0 != ex.unfinished_tasks(),
        "deadlock should have been detected"
    );
}

// TODO(rw): Decide which of these impls, if any, Executor and Spawner should have.
//#[test]
//fn impls_send_and_sync() {
//    assert!(impls::impls!(Executor: Send & Sync));
//}

#[test]
fn user_can_track_state() {
    let tracker = Rc::new(RefCell::new(Vec::new()));

    let mut ex = Executor::default();
    let spawner = ex.spawner();

    for i in 0..10 {
        let tracker = tracker.clone();
        spawner.spawn_detach(async move {
            tracker.borrow_mut().push(i);
        });
    }
    while ex.choose_any() {}

    let want: Vec<usize> = (0..10).collect();
    let got = {
        tracker.borrow_mut().sort_unstable();
        tracker.borrow_mut().clone()
    };
    assert_eq!(want, got);
}

#[test]
fn execution_is_deterministic() {
    async fn f(i: usize, tracker: Rc<RefCell<Vec<usize>>>) {
        yield_now().await;
        tracker.borrow_mut().push(i);
        yield_now().await;
        tracker.borrow_mut().push(i);
        yield_now().await;
        tracker.borrow_mut().push(i);
        yield_now().await;
    }
    let want = {
        let tracker: Rc<RefCell<Vec<usize>>> = Default::default();

        let mut ex = Executor::default();
        let spawner = ex.spawner();

        for i in 0..10 {
            let tracker = tracker.clone();
            spawner.spawn_detach(async move {
                f(i, tracker).await;
            });
        }
        while ex.choose_any() {}
        assert_eq!(0, ex.unfinished_tasks());

        let ret = tracker.borrow_mut().clone();
        ret
    };

    assert_eq!(30, want.len());

    for _ in 0..10 {
        let tracker: Rc<RefCell<Vec<usize>>> = Default::default();

        let mut ex = Executor::default();
        let spawner = ex.spawner();

        for i in 0..10 {
            let tracker = tracker.clone();
            spawner.spawn_detach(async move {
                f(i, tracker).await;
            });
        }
        while ex.choose_any() {}
        assert_eq!(0, ex.unfinished_tasks());

        let got = tracker.borrow_mut();

        assert_eq!(want, *got);
    }
}
