use std::cell::RefCell;
use std::rc::Rc;

use super::{utils::yield_now, Spawner};

/// The outcomes possible from [ChoiceSet::recv].
#[derive(PartialOrd, PartialEq, Eq, Ord, Copy, Clone, Debug)]
pub enum ChoiceSetRecvResult<T> {
    Empty,
    Lost(T),
    Received(T),
}

/// Model a one-way channel that receives messages, and nondeterministically loses them and/or
/// delivers them out-of-order.
///
/// TODO(rw): Model delivering a message more than once.
/// TODO(rw): Reduce the number of duplicated trajectories.
#[derive(Clone, Default, Debug)]
pub struct ChoiceSet<T> {
    items: Rc<RefCell<Vec<T>>>,
}

impl<T> ChoiceSet<T> {
    /// Clear the set of items.
    #[inline]
    pub fn reset(&self) {
        self.items.borrow_mut().clear();
    }

    /// Get a value from the set, using nondeterminism to decide whether to "lose" a message
    /// or deliver it. This models a one-way receiver (like UDP).
    ///
    /// Returns ChoiceSetRecvResult::Empty if the set is empty.
    ///
    /// Returns ChoiceSetRecvResult::Lost(item) if the item was popped from the set, and
    /// intended to be lost (this models a message that was lost in transit).
    ///
    /// Returns ChoiceSetRecvResult::Received(item) if the item was popped from the set, and
    /// intended to be delivered to the recipient (this models a message that was sent).
    #[inline]
    pub async fn recv(&self, spawner: &Spawner) -> ChoiceSetRecvResult<T> {
        yield_now().await;

        if self.items.borrow().is_empty() {
            ChoiceSetRecvResult::Empty
        } else {
            // borrow to get length, then release the borrow so that it is not held over a yield
            // point.
            let l = self.items.borrow().len();

            // Obtain the index of the item to remove, then remove it.
            let idx = spawner.hilberts_epsilon(l).await;
            let item = self.items.borrow_mut().swap_remove(idx);

            // Obtain the decision for whether to deliver or lose the item, then return the
            // outcome.
            if spawner.hilberts_epsilon(2).await == 0 {
                ChoiceSetRecvResult::Received(item)
            } else {
                ChoiceSetRecvResult::Lost(item)
            }
        }
    }

    /// Add a value to the set. Does not acknowledge receipt of messages because this is a one-way
    /// sender (like UDP).
    ///
    /// This has an internal async yield point so that the async simulator can decide when it runs.
    #[inline]
    pub async fn send(&self, item: T) {
        yield_now().await;
        self.items.borrow_mut().push(item);
    }
}
