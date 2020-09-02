//! A deterministic async task executor, with an emphasis on state-space exploration and
//! single-threaded execution speed.
//!
//! This code is a heavily modified and simplified fork of the LocalExecutor in stjepang's multitask crate.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use super::hilberts_epsilon::HilbertsEpsilon;
use super::waker;
use crate::KripkeStructure;

/// A unique identifier assigned to each task upon its creation.
///
/// Used for ordering tasks, so that execution is deterministic.
type TaskId = usize;

/// A detached future that the executor will run.
pub struct Task {
    id: TaskId,
    fut: Pin<Box<dyn Future<Output = ()>>>,
}

/// Store the inner state shared by an Executor, Spawner, and potentially other types in the
/// future.
#[derive(Default)]
struct Inner {
    /// Each task has an identifier. This stores the next identifier to use for a new task.
    task_id: TaskId,

    /// The collection of async tasks to poll. They are accessed by searching for the task that
    /// matches a given TaskId, not by a vector index.
    tasks: Vec<Task>,

    /// The collection of HilbertsEpsilons to evaluate. This collection, when fully evaluated,
    /// represents the "universe" the tasks are in. The number of choices in the next pending
    /// HilbertsEpsilon is what users get from a call to [choices], if applicable.
    hilberts_epsilons: Vec<HilbertsEpsilon>,

    /// The vector index of the next HilbertEpsilon to evaluate. If this equals the length of the
    /// collection of HilbertsEpsilons, then there are no remaining HilbertsEpsilons to evaluate.
    hilberts_epsilons_next: usize,

    /// The collection of task identifiers to choose from. An index into this vector is what users
    /// get from a call to [choices], if applicable.
    choices: Vec<TaskId>,

    /// The count of unfinished tasks. Useful for finding deadlocks and other logic errors.
    unfinished_tasks: isize,

    /// Track whether an invariant needs to be re-established in the choices vector.
    /// TODO(rw): Remove this?
    choices_dirty: bool,
}

/// A spawner for launching new tasks.
///
/// Use this when a user running a simulation should be prevented from modifying other Executor
/// state.
///
/// In the future, we may have other similar types that are used for reporting on, say, unfinished
/// tasks.
#[derive(Clone)]
pub struct Spawner {
    inner: Rc<RefCell<Inner>>,
}

impl Spawner {
    #[inline]
    pub fn spawn_detach(&self, fut: impl Future<Output = ()> + 'static) {
        // Allocate the task on the heap and turn it into a trait object.
        let fut: Pin<Box<dyn Future<Output = ()>>> = Box::pin(fut);

        // Borrow inner mutably, once for this function body.
        let mut inner = self.inner.borrow_mut();

        // Obtain this task's id.
        let id = inner.task_id;

        // Increment the task id counter, for the next task to use.
        inner.task_id += 1;

        // Store the task.
        let task = Task { fut, id };
        inner.tasks.push(task);

        // Store the task id.
        inner.choices.push(id);

        // Track this task as an unfinished task.
        inner.unfinished_tasks += 1;
    }

    /// Synchronously create a Hilbert's Epsilon operator, then return a future that fetches the
    /// result. The result will have been populated by the simulation while the task that called
    /// this function is awaited. (It is an internal logic error if the result was not
    /// populated.)
    ///
    /// Use this to create "parallel universes" in which the provided value is 0, 1, 2, .. up to
    /// the number of universes provided.
    ///
    /// This function is designed to not grow the state space more than the provided number of
    /// universes.
    ///
    /// The return value is a plain struct, not an impl Future, so that users can store the result
    /// future without boxing it.
    #[inline]
    pub fn hilberts_epsilon(&self, universes: usize) -> HilbertsEpsilonFuture {
        // The number of universes cannot be zero (then no universes exist!).
        assert!(universes >= 1, "HilbertEpsilon input must be nonzero");

        // Obtain the vector index to use to retrieve the result of this HilbertsEpsilon object.
        let idx = {
            // Create a new HilbertsEpsilon.
            let he = HilbertsEpsilon::new(universes);

            // Mutably borrow the inner state, store the HilbertsEpsilon, and obtain the vector
            // index at which the HilbertsEpsilon is stored. Because the vector is append-only, and
            // the vector pushes are deterministic, that index is stable.
            let mut inner = self.inner.borrow_mut();
            let idx = inner.hilberts_epsilons.len();
            inner.hilberts_epsilons.push(he);

            debug_assert!(inner.hilberts_epsilons[idx].choices().is_some());

            idx
        };

        // Return a future that yields to the scheduler, then fetches the universe chosen for this
        // HilbertsEpsilon. The scheduler must have picked a choice for the HilbertsEpsilon during
        // the time this function was yielded. The must_get call to the HilbertsEpsilon will panic
        // if not.
        HilbertsEpsilonFuture::new(self.inner.clone(), idx)
    }
}

/// An executor for stepping through concurrent computation using futures and tasks.
#[derive(Default)]
pub struct Executor {
    inner: Rc<RefCell<Inner>>,
}

// TODO(rw): impl UnwindSafe for Executor {} ?
// TODO(rw): impl RefUnwindSafe for Executor {} ?

impl Executor {
    /// Create a new Spawner. Clone it as many times as needed.
    #[inline]
    pub fn spawner(&self) -> Spawner {
        let inner = self.inner.clone();
        Spawner { inner }
    }

    /// Proceed one step in the computation by choosing arbitrarily from the available choices.
    ///
    /// The return value indicates if the trajectory is finished.
    #[inline]
    pub fn choose_any(&mut self) -> bool {
        let c = self.choices();
        if c >= 1 {
            // Choosing the last choice is faster because of an implementation detail:
            // Vec::swap_remove won't need to do two random reads, just one.
            self.choose(c - 1);
            true
        } else {
            false
        }
    }

    /// Choose the index of the next task to run for a step.
    ///
    /// If there are HilbertsEpsilons to choose from, then the argument identifies the universe to
    /// choose in the next pending HilbertsEpsilon. Otherwise, the argument identifies the location
    /// of the TaskId for the next task to poll.
    ///
    /// Valid inputs to this function are created by calling `choices`.
    ///
    /// TODO(rw): Explore using an enum to represent the choice identifier.
    #[inline]
    pub fn choose(&mut self, choice_identifier: usize) {
        // Get a mutable reference to the inner state that will be used to identify either the
        // HilbertsEpsilon to pick a value for, or to poll the indicated task.
        //
        // The code would be easier to read if this was borrowed separately for evaluating
        // HilbertsEpsilons and for polling tasks, but it's probably faster to minimize the number
        // of borrow calls.
        //
        // This borrow is intentionally dropped before a task is polled (if applicable), because
        // the task waker needs its own valid mutable borrow.
        let mut inner = self.inner.borrow_mut();

        // If any HilbertsEpsilons need choosing, find the first one that does, act on it, and
        // increment the index of the next HilbertsEpsilon that may need choosing.
        // (The ordering of the HilbertsEpsilons is deterministic, and this logic matches that in
        // the [choices] function, so this is deterministic, too.)
        {
            if inner.hilberts_epsilons_next < inner.hilberts_epsilons.len() {
                let he = {
                    let idx = inner.hilberts_epsilons_next;
                    &mut inner.hilberts_epsilons[idx]
                };

                debug_assert!(he.choices().is_some());
                he.choose(choice_identifier);
                debug_assert!(he.choices().is_none());

                // Increment the index of the next HilbertsEpsilon that may need choosing.
                inner.hilberts_epsilons_next += 1;
                return;
            }
        }

        // No HilbertsEpsilons needed acting upon, so interpret the choice identifier as the index
        // of the TaskId to poll.
        let mut task = {
            // Fetch the requested task id, removing it from the set of choices.
            // This action potentially leaves the task ids in an unordered state.
            let task_id = inner.choices.swap_remove(choice_identifier);

            // Fetch the requested task, removing it from the set of tasks.
            // This action potentially leaves the tasks in an unordered state.
            //
            // TODO(rw): For large numbers of pending tasks, it may be faster to use a binary
            // search to speed up the search. Or a very fast-to-insert hash table.
            let task_vec_id = {
                let maybe_task_vec_id: Option<usize> = inner
                    .tasks
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| t.id == task_id)
                    .map(|(i, _)| i)
                    .next();

                // If no task vec identifier was found, then the task that wanted to be woken up no
                // longer exists. As a result, this returns immediately without polling any tasks.
                //
                // Since the task_id has already been removed, we know that the set of choices has
                // changed. This prevents an infinite loop bug, whereby this same no-op choice
                // would be chosen repeatedly, forever.
                match maybe_task_vec_id {
                    None => unreachable!(),
                    Some(id) => id,
                }
            };

            // Take the task itself.
            let task = inner.tasks.swap_remove(task_vec_id);

            debug_assert_eq!(task_id, task.id);

            // Flag that an invariant may need to be re-established.
            inner.choices_dirty = true;

            task
        };

        // Drop the mutable borrow to the inner state, because the waker needs uncontended mutable
        // access to it.
        drop(inner);

        // Construct a waker to put this task id back into the choices set.
        //
        // Note that this works with task identifiers, not tasks. This separates waking and
        // enqueing tasks from owning tasks. As a result, potential race conditions are
        // reduced or eliminated.
        let w = waker::waker_fn({
            let inner = self.inner.clone();
            let task_id = task.id;
            move || {
                // TODO(rw): When this conditional is false, user code may have a problem. Perhaps
                // we can alert them that this is the case, without panicking via an .expect(...)
                // call.
                if let Ok(mut g) = inner.try_borrow_mut() {
                    g.choices.push(task_id);
                    g.choices_dirty = true;
                }
            }
        });

        {
            // Poll the task with the waker.
            let context = &mut Context::from_waker(&w);
            let poll_result = task.fut.as_mut().poll(context);

            // Process the poll result.
            // If ready, decrement the unfinished tasks counter.
            // If pending, put the task back onto the task collection, and dirty the choices set.
            let mut inner = self.inner.borrow_mut();
            match poll_result {
                Poll::Ready(()) => {
                    // Task finished, so decrement the count of unfinished tasks.
                    inner.unfinished_tasks -= 1;
                }
                Poll::Pending => {
                    // Put the task back since there is more work to do.
                    inner.tasks.push(task);
                    inner.choices_dirty = true;
                }
            }
        }

        // Collect the pending choices, which establishes the invariants needed when calling
        // `choices`.
        self.collect_pending_choices();
    }

    /// Calculate the number of available choices.
    ///
    /// If the result is zero, then the trajectory is finished.
    ///
    /// This function's result implies a range: 0..result. Any value in that range will be a valid
    /// argument to `choose`. (The range is left-inclusive and right-exclusive.)
    #[inline]
    pub fn choices(&mut self) -> usize {
        self.collect_pending_choices();

        let inner = self.inner.borrow();

        // Find the first HilbertsEpsilon, if any, that needs to be acted upon.
        // The returned value is the number of choices available in the found HilbertsEpsilon.
        // (The ordering of this is deterministic, and matches what the [choose] function requires.)
        if inner.hilberts_epsilons_next < inner.hilberts_epsilons.len() {
            let he_idx = inner.hilberts_epsilons_next;
            let he = &inner.hilberts_epsilons[he_idx];
            return he
                .choices()
                .expect("logic error: the HilbertsEpsilon should have available choices");
        }

        // No HilbertsEpsilons needed acting upon, so return the number of async tasks available to
        // poll.
        inner.choices.len()
    }

    /// Reset the inner state.
    ///
    /// Clears the choices set, the count of unfinished tasks, and other internal state.
    ///
    /// Along with restarting the execution, this helps to reduce heap allocations during
    /// repeated simulation runs.
    #[inline]
    pub fn reset(&mut self) {
        let mut inner = self.inner.borrow_mut();

        // Reset the task id.
        inner.task_id = 0;

        // Clear the existing tasks.
        inner.tasks.clear();

        // Clear the hilberts epsilons.
        inner.hilberts_epsilons.clear();
        inner.hilberts_epsilons_next = 0;

        // Clear the existing choices.
        inner.choices.clear();
        debug_assert_eq!(
            0,
            inner.choices.len(),
            "a task woke up after the executor was reset, your code may be using real time or background threads"
        );

        // Reset the number of unfinished_tasks.
        inner.unfinished_tasks = 0;

        // Reset the dirty flag.
        inner.choices_dirty = false;
    }

    /// Obtain the number of unfinished tasks.
    ///
    /// If the number is positive, and the trajectory is finished, then there this is number of
    /// idle tasks.
    ///
    /// If the number is zero, then there are no idle tasks.
    ///
    /// If the number is negative, then there is a logic error.
    #[inline]
    pub fn unfinished_tasks(&self) -> isize {
        self.inner.borrow().unfinished_tasks
    }

    /// Collect the tasks that scheduled themselves, and return the number of choices.
    ///
    /// Sorts the full choices vector for determinism.
    #[inline]
    fn collect_pending_choices(&mut self) -> usize {
        let mut inner = self.inner.borrow_mut();
        if inner.choices_dirty {
            // TODO(rw): Try replacing the choices vec with a BTreeMap or rank/select data structure.
            // TODO(rw): Double-verify that sorting is no longer necessary.
            // TODO(rw): If sorting is no longer necessary, remove this function and the dirty
            // flag.
            // inner.choices.sort_unstable();
            // inner.tasks.sort_unstable();

            inner.choices_dirty = false;
        }
        inner.choices.len()
    }
}

/// A future that fetches the value chosen in a "parallel universe" during simulation.
///
/// The Executor manages the actual choices made for each universe.
///
/// Created by calling [Executor::hilberts_epsilon].
///
/// Invariant: If this future is ready to be polled, then the Executor must have already populated
/// the value. The getter function will panic if this is not true.
#[derive(Clone)]
pub struct HilbertsEpsilonFuture {
    inner: Rc<RefCell<Inner>>,
    idx_of_underlying_hilberts_epsilon: usize,
    yielded: bool,
}

impl HilbertsEpsilonFuture {
    #[inline]
    fn new(inner: Rc<RefCell<Inner>>, idx_of_underlying_hilberts_epsilon: usize) -> Self {
        Self {
            inner,
            idx_of_underlying_hilberts_epsilon,
            yielded: false,
        }
    }
}

impl std::fmt::Debug for HilbertsEpsilonFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HilbertsEpsilonFuture")
            .field(
                "idx_of_underlying_hilberts_epsilon",
                &self.idx_of_underlying_hilberts_epsilon,
            )
            .finish()
    }
}

impl Future for HilbertsEpsilonFuture {
    type Output = usize;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this: &mut Self = Pin::into_inner(self);

        // If this future has not yielded yet, do so, so that the Executor has a chance to do what
        // it must: pick the value of the HilbertsEpsilon.
        if !this.yielded {
            this.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        // If this future has already yielded, then the Executor must have chosen a value for it.
        // Therefore, it is always true that there is a value to return.
        // (If this invariant is not satisfied, must_get will panic.)
        } else {
            let inner = this.inner.borrow();
            let idx = this.idx_of_underlying_hilberts_epsilon;
            let val = inner.hilberts_epsilons[idx].must_get();
            Poll::Ready(val)
        }
    }
}

impl KripkeStructure for Executor {
    // TODO(rw): Consider reifying the Label type into something richer, that contains details of
    // HilbertsEpsilon and other things. This will likely involve enlarging the Label type (by
    // making it an enum, and associating other data with it), which may have speed implications;
    // or, storing vector indexes as u32.
    type Label = usize;
    type LabelIterator = std::ops::Range<usize>;

    #[inline]
    fn transition(&mut self, label: Self::Label) {
        self.choose(label);
    }

    #[inline]
    fn successors(&mut self) -> Option<Self::LabelIterator> {
        match self.choices() {
            0 => None,
            n => Some(0..n),
        }
    }

    #[inline]
    fn restart(&mut self) {
        self.reset();
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task").field("id", &self.id).finish()
    }
}

impl std::cmp::PartialEq for Task {
    #[inline(always)]
    fn eq(&self, o: &Self) -> bool {
        self.id.eq(&o.id)
    }
}

impl std::cmp::Eq for Task {}

impl std::cmp::PartialOrd for Task {
    #[inline(always)]
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&o.id)
    }
}

impl std::cmp::Ord for Task {
    #[inline(always)]
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.id.cmp(&o.id)
    }
}
