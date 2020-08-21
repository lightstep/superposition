//! Implements an epsilon "choice" operator as a [futures_lite::Stream].

use futures_lite::Stream;

use std::marker::Unpin;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::futures::Executor;

/// An Epsilon operator for async nondeterminism, for use as a [Stream].
///
/// A [Stream] is effectively an asynchronous [Iterator]. Although the `.next()`
/// method of the stream is an async function, a stream internally uses synchronous
/// polling. So if a choice operation must happen in a stream context, the choice
/// must use fully-synchronous code. That means
/// [hilberts_epsilon](crate::futures::hilberts_epsilon::hilberts_epsilon) cannot be used.
///
/// Instead, we take advantage of the fact that the action of synchronously polling
/// a stream is performed by an asynchronous task that is itself subject to reordering
/// in the task queue. By allowing the stream's underlying task to poll once per choice,
/// and by using a second task that sets a flag stopping polling, the relative ordering
/// of these tasks is used to deterministically select a choice.
///
/// # Example
///
/// ```
/// # use superposition::futures::ChoiceStream;
/// use futures_lite::StreamExt; // Provides .next().
///
/// # let executor = superposition::futures::Executor::default();
/// # let ex = &executor;
/// let mut stream = ChoiceStream::new(ex, 0..5);
///
/// // Polling the stream yields the choice.
/// ex.spawn_detach(async move {
///     let choice: usize = stream.next().await.unwrap();
///     assert!(choice < 5);
/// });
/// ```
#[derive(Debug)]
pub struct ChoiceStream<I, X>
where
    I: Iterator<Item = X> + Unpin + Send + 'static,
    X: Unpin + Send + Copy + 'static,
{
    /// Flag representing whether the current value of `choice` is finalized.
    ///
    /// The flag is set by an async background process. When the stream is polled,
    /// if the flag is set or the maximum choice is encountered, the choice is yielded.
    is_finalized: Arc<AtomicBool>,

    /// An owned iterator over all possible choices.
    choices: I,

    /// The current element yielded by the iterator. Initialized to the first element.
    cur_elem: Option<X>,

    /// The next element, relative to the current, yielded by the iterator.
    ///
    /// This is used to detect the case of `cur_elem` being the last element early,
    /// which prevents the last choice from being reached in two different ways.
    next_elem: Option<X>,

    /// True iff the stream has already yielded a choice.
    ///
    /// This restricts a ChoiceStream to producing a single value.
    yielded_choice: bool,
}

impl<I, X> ChoiceStream<I, X>
where
    I: Iterator<Item = X> + Unpin + Send + 'static,
    X: Unpin + Send + Copy + 'static,
{
    /// Creates a new [ChoiceStream].
    pub fn new(ex: &Executor, mut choices: I) -> Self {
        let cur_elem = choices.next();
        let next_elem = choices.next();

        // Ensure at least two choices.
        assert!(cur_elem.is_some());
        assert!(next_elem.is_some());

        let stream = ChoiceStream {
            is_finalized: Arc::default(),
            choices,
            cur_elem,
            next_elem,
            yielded_choice: false,
        };

        // Spawn a background task that locks in the current choice.
        ex.spawn_detach({
            let is_finalized = stream.is_finalized.clone();
            async move {
                is_finalized.store(true, Ordering::SeqCst);
            }
        });

        // The caller must now call `.next().await` to insert the stream
        // into the executor task queue.
        stream
    }
}

impl<I, X> Stream for ChoiceStream<I, X>
where
    I: Iterator<Item = X> + Unpin + Send + 'static,
    X: Unpin + Send + Copy + 'static,
{
    type Item = X;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut Self = Pin::into_inner(self);

        // If the stream already yielded a choice, do not yield another.
        if this.yielded_choice {
            return Poll::Ready(None);
        }

        // If the flag was set by the other task, return the current element.
        if this.is_finalized.load(Ordering::SeqCst) {
            this.yielded_choice = true;
            return Poll::Ready(this.cur_elem);
        }

        // Advance the iterator to the next element.
        this.cur_elem = this.next_elem;
        this.next_elem = this.choices.next();

        // If this is the last element, yield it eagerly.
        if this.next_elem.is_none() {
            this.yielded_choice = true;
            Poll::Ready(this.cur_elem)
        } else {
            cx.waker().wake_by_ref(); // Re-schedule the task.
            Poll::Pending
        }
    }
}
