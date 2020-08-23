//! A deterministic async task executor, with an emphasis on state-space exploration and
//! single-threaded execution speed.
//!
//! This code is a heavily modified and simplified fork of the LocalExecutor in stjepang's multitask crate.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

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

#[derive(Default)]
struct Inner {
    task_id: TaskId,
    tasks: Vec<Task>,
    choices: Vec<TaskId>,
    unfinished_tasks: isize,
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
    /// Panics if the idx is larger than the number of woken tasks.
    ///
    /// Valid inputs to this function are created by calling `choices`.
    #[inline]
    pub fn choose(&mut self, idx: usize) {
        let mut task = {
            let mut inner = self.inner.borrow_mut();

            // Fetch the requested task id, removing it from the set of choices.
            // This action potentially leaves the task ids in an unordered state.
            let task_id = inner.choices.swap_remove(idx);

            // Fetch the requested task, removing it from the set of tasks.
            // This action potentially leaves the tasks in an unordered state.
            let task_vec_id = {
                inner
                    .tasks
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| t.id == task_id)
                    .map(|(i, _)| i)
                    .next()
                    .expect("logic error")
            };

            // Take the task itself.
            let task = inner.tasks.swap_remove(task_vec_id);

            debug_assert_eq!(task_id, task.id);

            // Flag that an invariant may need to be re-established.
            inner.choices_dirty = true;

            task
        };

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
                // we can alert them that this is the case.
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
    /// argument to `choose`.
    #[inline]
    pub fn choices(&mut self) -> usize {
        self.collect_pending_choices()
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

impl KripkeStructure for Executor {
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
