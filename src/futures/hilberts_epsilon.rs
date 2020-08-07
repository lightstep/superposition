use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use super::utils::yield_now;
use super::Executor;

// Re-export the iproduct macro from itertools.
//
// This macro is an ergonomic way to create the Cartesian product of multiple iterators,
// especially when their types are heterogenous.
pub use itertools::iproduct;

/// Hilbert's "Epsilon Operator" for async nondeterminism.
///
/// In simulation, this will create trajectories for each choice.
///
/// The current implementation does not rely on any specific detail of the Executor.
///
/// Because of this, multiple calls to this function will drastically enlarge the state space.
///
/// The implementation uses one spawned task that advances the iterator in the background. When the
/// foreground task fetches the current value, the background iterator is dropped.
///
/// An alternative implementation that is conceptually more elegant would be to spawn a task for
/// every option, then wait on a "select" operation to choose the first one that is ready. However,
/// that probably causes much higher memory usage, and wil not be practical with large choice sets.
#[inline]
pub async fn hilberts_epsilon<I, X>(ex: Executor, mut choices: I) -> X
where
    I: Iterator<Item = X> + Send + 'static,
    X: Send + Copy + 'static,
{
    let flag = Arc::new(AtomicBool::new(false));

    // Spawn the background process that may eventually toggle the slot to true.
    ex.spawn({
        let flag = flag.clone();
        async move {
            flag.store(true, Ordering::SeqCst);
        }
    })
    .detach();

    // Iterate through the choices, returning the first element that is seen after the flag is set
    // to true. If the flag is never set to true, return the last element in the iterator.
    let mut last_elem = choices.next().expect("choice set must be nonempty");
    for elem in choices {
        yield_now().await;
        if flag.load(Ordering::SeqCst) {
            return last_elem;
        }
        last_elem = elem;
    }
    last_elem
}
