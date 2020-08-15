//! A deterministic async task executor, with an emphasis on state-space exploration.
//!
//! This code is a heavily modified and simplified fork of the LocalExecutor in stjepang's multitask crate.

use std::collections::HashMap;
use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
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
        unreachable!("TODO(rw): double-check this for correctness");
        // let mut task = self;
        // let handle = task.0.take().unwrap();
        // handle.cancel();
        // handle.await
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
    choices: Arc<Mutex<Vec<TaskId>>>,

    /// The task choices queue. This is lock-free, which mitigates the deadlocks that can occur
    /// when pushing to the choices Mutex-wrapped Vec.
    choices_queue: Arc<ConcurrentQueue<TaskId>>,

    /// The tasks themselves.
    tasks: Arc<Mutex<HashMap<TaskId, Runnable>>>,

    /// Count of unfinished tasks. Used for deadlock detection.
    unfinished_tasks: Arc<AtomicIsize>,

    /// Flag to indicate if the choices vec is accepting new tasks.
    accepting_choices: Arc<AtomicBool>,

    /// Flag to indicate whether the choices_queue needs draining.
    /// TODO(rw): Remove this if we move to a Sync queue with less overhead than ConcurrentQueue.
    choices_queue_dirty: Arc<AtomicBool>,
}

impl UnwindSafe for Executor {}
impl RefUnwindSafe for Executor {}

impl Default for Executor {
    fn default() -> Executor {
        Executor {
            task_id: Arc::new(AtomicUsize::new(0)),
            choices: Arc::new(Mutex::new(Vec::new())),
            choices_queue: Arc::new(ConcurrentQueue::unbounded()),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            unfinished_tasks: Arc::new(AtomicIsize::new(0)),
            accepting_choices: Arc::new(AtomicBool::new(true)),
            choices_queue_dirty: Arc::new(AtomicBool::new(false)),
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
            let choices = self.choices.clone();
            let choices_queue = self.choices_queue.clone();
            let tasks = self.tasks.clone();
            let accepting_choices = self.accepting_choices.clone();
            let choices_queue_dirty = self.choices_queue_dirty.clone();

            // NOTE(rw): async-task promises that this won't be called if a task is dropped
            // before being run. Which, I think, implies that it can't cause an infinite loop
            // of task creation.
            move |runnable| {
                if accepting_choices.load(Ordering::SeqCst) {
                    tasks.lock().insert(task_id, runnable);
                    // Try to push the runnable on to the primary choices set, which has the least
                    // overhead. If that object is already locked, push the runnable to the choices
                    // queue and mark the choices queue as dirty.
                    match choices.try_lock() {
                        Some(mut guard) => {
                            guard.push(task_id);
                        }
                        None => {
                            choices_queue
                                .push(task_id)
                                .expect("internal queue depth too large");
                            choices_queue_dirty.store(true, Ordering::SeqCst);
                        }
                    }
                }
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
    pub fn choose(&self, idx: usize) {
        // Assert that the choices queue is empty, because 1) it should have been drained already,
        // and 2) if it has tasks in it, then it's possible that user code is creating tasks
        // out-of-band (such as by using real time or background threads).
        debug_assert_eq!(0, self.choices_queue.len());

        // Fetch the requested task id, removing it from the set of choices.
        let task_id = self.choices.lock().swap_remove(idx);

        // Fetch the requested task, removing it from the set of tasks.
        let task = self
            .tasks
            .lock()
            .remove(&task_id)
            .expect("logic error: task missing");

        // Poll the requested task.
        task.run();

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
    pub fn choices(&self) -> usize {
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
        // Stop accepting choices, for the duration of this function.
        self.accepting_choices.store(false, Ordering::SeqCst);

        // Clear the existing choices.
        self.choices.lock().clear();

        // Clear the existing tasks.
        self.tasks.lock().clear();

        // Clear the choices queue.
        // This should be finite because dropped tasks cannot be scheduled again, according to the
        // behavior of the async-task library.
        // NOTE(rw): Re-verify this reasoning.
        if self.choices_queue_dirty.load(Ordering::SeqCst) {
            while self.choices_queue.pop().is_ok() {}
            self.choices_queue_dirty.store(false, Ordering::SeqCst);
        }

        debug_assert_eq!(
            0,
            self.choices.lock().len(),
            "a task woke up after the executor was reset, your code may be using real time"
        );

        // Reset the number of unfinished_tasks.
        self.unfinished_tasks.store(0, Ordering::SeqCst);

        // Start accepting choices again.
        self.accepting_choices.store(true, Ordering::SeqCst);
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
        // Drain the choices_queue into the choices vec.
        let mut choices = self.choices.lock();

        // TODO(rw): remove choices_queue_dirty once we use a queue with less overhead.
        if self.choices_queue_dirty.load(Ordering::SeqCst) {
            // Drain only as many tasks from the queue as there are to start with. This prevents a
            // possible infinite loop wherein tasks are spawning tasks on drop.
            let l = self.choices_queue.len();
            for _ in 0..l {
                if let Ok(r) = self.choices_queue.pop() {
                    choices.push(r);
                }
            }

            debug_assert_eq!(
                0,
                self.choices_queue.len(),
                "your tasks are creating new tasks on drop"
            );
        }

        // For determinism, sort tasks by the tag, which is the unique task id.
        // TODO(rw): Try replacing the choices vec with a BTreeMap or rank/select data structure.
        choices.sort();
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
