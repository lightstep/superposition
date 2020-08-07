/// Re-export an implementation of an async function to yield immediately.
pub use futures_lite::future::yield_now;

/// Wrap a function call with an async yield_now.
///
/// Use this to:
///
///   1) Wrap synchronous communication primitives (like `Arc<AtomicIsize>`) in an async block, so
///      that the scheduler yields when it should, or
///
///   2) Enlarge the state space that a simulation will explire.
#[inline]
pub async fn asyncify<F: FnOnce() -> T, T>(f: F) -> T {
    yield_now().await;
    f()
}
