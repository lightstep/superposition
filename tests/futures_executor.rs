use std::sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
};

use superposition::futures::{sync::AsyncMutex, utils::yield_now, Executor};

#[test]
fn drives_concurrent_locks_to_completion() {
    let ex = Executor::default();

    let counter = Arc::new(AtomicIsize::new(0));

    ex.spawn({
        let lock = Arc::new(AsyncMutex::new(()));

        let ex = ex.clone();
        let counter = counter.clone();

        async move {
            for _ in 0..1000 {
                let counter = counter.clone();
                let lock = lock.clone();
                ex.spawn({
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
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .detach();
            }
        }
    })
    .detach();

    while ex.choose_any() {}

    assert_eq!(0, ex.choices());
    assert_eq!(0, ex.unfinished_tasks());

    assert_eq!(1000, counter.load(Ordering::SeqCst));
}

#[test]
fn detects_deadlock() {
    let ex = Executor::default();
    ex.spawn({
        let m = Arc::new(AsyncMutex::new(()));
        async move {
            let _a = m.lock().await;
            let _b = m.lock().await;
        }
    })
    .detach();
    while ex.choose_any() {}
    assert!(
        0 != ex.unfinished_tasks(),
        "deadlock should have been detected"
    );
}

#[test]
fn impls_send_and_sync() {
    assert!(impls::impls!(Executor: Send & Sync));
}

#[test]
fn user_can_track_state() {
    let tracker = Arc::new(std::sync::Mutex::new(Vec::new()));

    let ex = Executor::default();

    for i in 0..10 {
        let tracker = tracker.clone();
        ex.spawn(async move {
            tracker.lock().unwrap().push(i);
        })
        .detach();
    }
    while ex.choose_any() {}

    let want: Vec<usize> = (0..10).collect();
    let got = {
        tracker.lock().unwrap().sort();
        tracker.lock().unwrap().clone()
    };
    assert_eq!(want, got);
}

#[test]
fn execution_is_deterministic() {
    async fn f(i: usize, tracker: Arc<std::sync::Mutex<Vec<usize>>>) {
        yield_now().await;
        tracker.lock().unwrap().push(i);
        yield_now().await;
        tracker.lock().unwrap().push(i);
        yield_now().await;
        tracker.lock().unwrap().push(i);
        yield_now().await;
    }
    let want = {
        let tracker: Arc<std::sync::Mutex<Vec<usize>>> = Default::default();

        let ex = Executor::default();

        for i in 0..10 {
            let tracker = tracker.clone();
            ex.spawn(async move {
                f(i, tracker).await;
            })
            .detach();
        }
        while ex.choose_any() {}
        assert_eq!(0, ex.unfinished_tasks());

        let ret = tracker.lock().unwrap().clone();
        ret
    };

    assert_eq!(30, want.len());

    for _ in 0..10 {
        let tracker: Arc<std::sync::Mutex<Vec<usize>>> = Default::default();

        let ex = Executor::default();

        for i in 0..10 {
            let tracker = tracker.clone();
            ex.spawn(async move {
                f(i, tracker).await;
            })
            .detach();
        }
        while ex.choose_any() {}
        assert_eq!(0, ex.unfinished_tasks());

        let got = tracker.lock().unwrap();

        assert_eq!(want, *got);
    }
}
