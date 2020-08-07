//! A deterministic async task executor, with an emphasis on state-space exploration.
//!
//! This code is a heavily modified and simplified fork of the LocalExecutor in stjepang's multitask crate.

use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicIsize, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use concurrent_queue::ConcurrentQueue;
use spin::Mutex;

use super::on_ready_fn::OnReadyFn;
use crate::KripkeStructure;

/// A unique identifier assigned to each task upon its creation.
///
/// Used for ordering tasks, so that execution is deterministic.
type TaskId = usize;

/// Documentation from stjepang:
///
/// A runnable future, ready for execution.
///
/// When a future is internally spawned using `async_task::spawn()` or `async_task::spawn_local()`,
/// we get back two values:
///
/// 1. an `async_task::Task<()>`, which we refer to as a `Runnable`
/// 2. an `async_task::JoinHandle<T, ()>`, which is wrapped inside a `Task<T>`
///
/// Once a `Runnable` is run, it "vanishes" and only reappears when its future is woken. When it's
/// woken up, its schedule function is called, which means the `Runnable` gets pushed into a task
/// queue in an executor.
type Runnable = async_task::Task<TaskId>;

/// Documentation from stjepang:
///
/// A spawned future.
///
/// Tasks are also futures themselves and yield the output of the spawned future.
///
/// When a task is dropped, its gets canceled and won't be polled again. To cancel a task a bit
/// more gracefully and wait until it stops running, use the [`cancel()`][Task::cancel()] method.
///
/// Tasks that panic get immediately canceled. Awaiting a canceled task also causes a panic.
///
/// If a task panics, the panic will be thrown by the [`Ticker::tick()`] invocation that polled it.
#[must_use = "tasks get canceled when dropped, use `.detach()` to run them in the background"]
#[derive(Debug)]
pub struct Task<T>(Option<async_task::JoinHandle<T, TaskId>>);

impl<T> Task<T> {
    /// Documentation from stjepang:
    ///
    /// Detaches the task to let it keep running in the background.
    #[inline]
    pub fn detach(mut self) {
        self.0.take().unwrap();
    }

    /// Documentation from stjepang:
    ///
    /// Cancels the task and waits for it to stop running.
    ///
    /// Returns the task's output if it was completed just before it got canceled, or [`None`] if
    /// it didn't complete.
    ///
    /// While it's possible to simply drop the [`Task`] to cancel it, this is a cleaner way of
    /// canceling because it also waits for the task to stop running.
    #[inline]
    pub async fn cancel(self) -> Option<T> {
        let mut task = self;
        let handle = task.0.take().unwrap();
        handle.cancel();
        handle.await
    }
}

impl<T> Drop for Task<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.cancel();
        }
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0.as_mut().unwrap()).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready(output.expect("task has failed")),
        }
    }
}

/// An executor for stepping through concurrent computation using futures and tasks.
#[derive(Clone, Debug)]
pub struct Executor {
    /// Next task_id. Used for uniquely identifying tasks.
    task_id: Arc<AtomicUsize>,

    /// The task choices.
    choices: Arc<Mutex<Vec<Runnable>>>,

    /// The task choices queue. This is lock-free, which mitigates the deadlocks that can occur
    /// when pushing to the choices Mutex-wrapped Vec.
    choices_queue: Arc<ConcurrentQueue<Runnable>>,

    /// Count of unfinished tasks. Used for deadlock detection.
    unfinished_tasks: Arc<AtomicIsize>,
}

impl UnwindSafe for Executor {}
impl RefUnwindSafe for Executor {}

impl Default for Executor {
    fn default() -> Executor {
        Executor {
            task_id: Arc::new(AtomicUsize::new(0)),
            choices: Arc::new(Mutex::new(Vec::new())),
            choices_queue: Arc::new(ConcurrentQueue::unbounded()),
            unfinished_tasks: Arc::new(AtomicIsize::new(0)),
        }
    }
}

impl Executor {
    /// Spawn a future, making it available as a choice for deterministic execution.
    ///
    ///
    /// Documentation from stjepang:
    ///
    /// Spawns a future onto this executor.
    ///
    /// Returns a [`Task`] handle for the spawned future.
    #[inline]
    pub fn spawn<T: 'static + Send>(
        &self,
        future: impl Future<Output = T> + 'static + Send,
    ) -> Task<T> {
        // Obtain the next task id, to facilitate deterministic task ordering.
        let task_id = self.task_id.fetch_add(1, Ordering::SeqCst);

        // Increment the unfinished_tasks counter to facilitate deadlock detection.
        let unfinished_tasks = self.unfinished_tasks.clone();
        unfinished_tasks.fetch_add(1, Ordering::SeqCst);

        // The function that schedules a runnable task when it gets woken up.
        let schedule = {
            let choices_queue = self.choices_queue.clone();

            move |runnable| {
                // NOTE(rw): async-task promises that this won't be called if a task is dropped
                // before being run.
                choices_queue
                    .push(runnable)
                    .expect("internal queue depth too large");
            }
        };

        // Wrap the future so that, when it finishes, it decrements the unfinished_tasks counter.
        // (This seems more robust than decrementing the counter in Task's Drop, or at the moment of
        // polling. This is primarily because detached Tasks don't call our Drop when they are
        // dropped.)
        //
        // By using OnReadyFn, this is guaranteed to not enlarge the state space.
        let future = OnReadyFn::new(future, move || {
            unfinished_tasks.fetch_sub(1, Ordering::SeqCst);
        });

        // Create a task, push it into the queue by scheduling it, and return its `Task` handle.
        let (runnable, handle) = async_task::spawn(future, schedule, task_id);
        runnable.schedule();
        Task(Some(handle))
    }

    /// Proceed one step in the computation by choosing arbitrarily from the available choices.
    ///
    /// The return value indicates if the trajectory is finished.
    #[inline]
    pub fn choose_any(&self) -> bool {
        self.collect_pending_choices();
        let c = self.choices();
        assert_eq!(0, self.choices_queue.len());
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
    pub fn choose(&self, idx: usize) {
        self.collect_pending_choices();
        assert_eq!(0, self.choices_queue.len());
        let r = self.choices.lock().swap_remove(idx);
        r.run();
    }

    /// Calculate the number of available choices.
    ///
    /// If the result is zero, then the trajectory is finished.
    ///
    /// This function's result implies a range: 0..result. Any value in that range will be a valid
    /// argument to `choose`.
    #[inline]
    pub fn choices(&self) -> usize {
        self.collect_pending_choices();
        self.choices.lock().len()
    }

    /// Reset the inner state.
    ///
    /// Clears the pending choices queue, the choices set, and the count of unfinished tasks.
    ///
    /// Along with restarting the execution, this helps to reduce heap allocations during
    /// repeated simulation runs.
    #[inline]
    pub fn reset(&self) {
        // Clear the existing choices.
        self.choices.lock().clear();

        // Clear the choices queue.
        // This should be finite because dropped tasks cannot be scheduled again, according to the
        // behavior of the async-task library.
        // NOTE(rw): Re-verify this reasoning.
        while self.choices_queue.pop().is_ok() {}

        // Reset the number of unfinished_tasks.
        self.unfinished_tasks.store(0, Ordering::SeqCst);

        assert_eq!(
            0,
            self.choices.lock().len(),
            "a task woke up after the executor was reset, your code may be using real time"
        );
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
        self.unfinished_tasks.load(Ordering::SeqCst)
    }

    /// Collect the tasks that scheduled themselves.
    ///
    /// Sorts the full choices vector for determinism.
    #[inline]
    pub fn collect_pending_choices(&self) {
        let mut choices = self.choices.lock();

        // Drain the choices_queue into the choices vec.
        let mut pushed = false;
        while let Ok(r) = self.choices_queue.pop() {
            choices.push(r);
            pushed |= true;
        }

        // For determinism, sort tasks by the tag, which is the unique task id.
        // TODO(rw): Try replacing the choices vec with a BTreeMap or rank/select data structure.
        if pushed {
            choices.sort_by_key(|t| *t.tag());
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        // TODO(stjepang): Close the local queue and empty it.
        // TODO(stjepang): Cancel all remaining tasks.
    }
}

impl KripkeStructure for &Executor {
    type Label = usize;
    type LabelIterator = std::ops::Range<usize>;

    #[inline]
    fn transition(self, label: Self::Label) {
        self.choose(label);
    }

    #[inline]
    fn successors(self) -> Option<Self::LabelIterator> {
        match self.choices() {
            0 => None,
            n => Some(0..n),
        }
    }

    #[inline]
    fn restart(self) {
        self.reset();
    }
}
