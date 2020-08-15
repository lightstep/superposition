/// This re-export is deprecated and will be replaced.
///
/// Re-export an implementation of an async mutex.
///
/// Note that this Mutex implementation was chosen because it has no dependencies on real time.
///
/// Other crates, such as async-lock and async-mutex, depend on real time, so they are unsuitable.
pub use futures_util::lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

/// Re-export an implementation of an intrusive async mutex.
///
/// It is probably faster than the Mutex from futures_util, although it is harder to use.
///
/// Currently, it expects a second argument in its initializer: whether the lock is fair or not.
///
/// For exhaustive simulation purposes, fairness probably doesn't matter.
///
/// Note that this Mutex implementation was chosen because it has no dependencies on real time.
///
/// Other crates, such as async-lock and async-mutex, depend on real time, so they are unsuitable.
///
/// This Mutex implementation was also chosen because it minimizes heap allocations, which is very
/// important for the intended use cases of this library.
///
/// For example, when the async-mutex crate was used instead, the worst-case timeout of 500
/// microseconds was only triggered once, but a new std::time::Instant was created on every
/// locking attempt. This caused significant and unnecessary overhead during simulation.
pub use futures_intrusive::sync::{
    Mutex as IntrusiveAsyncMutex, MutexGuard as IntrusiveAsyncMutexGuard,
};
