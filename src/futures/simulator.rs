use std::sync::Arc;

use spin::Mutex;

use crate::futures::{Controller, Executor};
use crate::KripkeStructure;

/// Combine an Executor with a Controller to deterministically and repeatedly run async code.
#[derive(Clone)]
pub struct Simulator<C> {
    executor: Executor,
    controller: Arc<Mutex<Option<C>>>,
}

impl<C> Simulator<C> {
    pub fn new(controller: C) -> Self {
        let executor = Executor::default();
        let controller = Arc::new(Mutex::new(Some(controller)));
        Self {
            executor,
            controller,
        }
    }

    #[inline]
    pub fn take_controller(&self) -> C {
        self.controller.lock().take().unwrap()
    }

    #[inline]
    pub fn put_controller(&self, c: C) {
        self.controller.lock().replace(c);
    }
}

/// Default for Simulator needs to be customized so that the owned Controller is created and stored
/// as Some, instead of as None.
impl<C> Default for Simulator<C>
where
    C: Default,
{
    fn default() -> Self {
        let executor = Executor::default();
        let controller = C::default();
        let controller = Arc::new(Mutex::new(Some(controller)));
        Self {
            executor,
            controller,
        }
    }
}

impl<C> KripkeStructure for &Simulator<C>
where
    C: Controller,
{
    type Label = usize;
    type LabelIterator = std::ops::Range<usize>;

    #[inline]
    fn transition(self, label: Self::Label) {
        self.executor.choose(label);

        let c = self.take_controller();
        let c = c.on_transition();
        self.put_controller(c);
    }

    #[inline]
    fn successors(self) -> Option<Self::LabelIterator> {
        let n = self.executor.choices();

        if n == 0 {
            let c = self.take_controller();
            let c = c.on_end_of_trajectory(&self.executor);
            self.put_controller(c);
            None
        } else {
            Some(0..n)
        }
    }

    #[inline]
    fn restart(self) {
        self.executor.reset();

        let c = self.take_controller();
        let c = c.on_restart(&self.executor);
        self.put_controller(c);
    }
}
