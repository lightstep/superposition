use crate::futures::{Controller, Executor, Spawner};
use crate::KripkeStructure;

/// Combine an Executor with a Controller to deterministically and repeatedly run async code.
pub struct Simulator<C> {
    executor: Executor,
    controller: Option<C>,
    spawner: Spawner,
}

impl<C> Simulator<C> {
    /// Create a new Simulator using an existing Controller.
    pub fn new(controller: C) -> Self {
        let executor = Executor::default();
        let spawner = executor.spawner();
        let controller = Some(controller);
        Self {
            executor,
            spawner,
            controller,
        }
    }

    /// Take the controller. Panics if the Controller has already been taken.
    #[inline]
    pub fn take_controller(&mut self) -> C {
        self.controller.take().unwrap()
    }

    /// Put the controller back.
    #[inline]
    pub fn put_controller(&mut self, c: C) {
        self.controller.replace(c);
    }
}

/// Implement Default for Simulator so that the inner Option<Controller> is populated.
impl<C> Default for Simulator<C>
where
    C: Default,
{
    fn default() -> Self {
        let executor = Executor::default();
        let spawner = executor.spawner();
        let controller = Some(C::default());
        Self {
            executor,
            spawner,
            controller,
        }
    }
}

impl<C> KripkeStructure for Simulator<C>
where
    C: Controller,
{
    type Label = usize;
    type LabelIterator = std::ops::Range<usize>;

    #[inline]
    fn transition(&mut self, label: Self::Label) {
        let choice_taken = self.executor.choose(label);

        if let Some(c) = self.controller.as_mut() {
            c.on_transition(choice_taken);
        }
    }

    #[inline]
    fn successors(&mut self) -> Option<Self::LabelIterator> {
        let n = self.executor.choices();

        if n == 0 {
            if let Some(c) = self.controller.as_mut() {
                let e = &self.executor;
                c.on_end_of_trajectory(e);
            }
            None
        } else {
            Some(0..n)
        }
    }

    #[inline]
    fn restart(&mut self) {
        self.executor.reset();

        if let Some(c) = self.controller.as_mut() {
            c.on_restart(&self.spawner);
        }
    }
}
