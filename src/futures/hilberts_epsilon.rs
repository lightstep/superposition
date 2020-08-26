/// Hilbert's "Epsilon Operator" for async nondeterminism.
///
/// It is used by the simulation runtime and is intended to be opaque to the user.
///
/// To use HilbertsEpsilon, call it using [Spawner::hilberts_epsilon].
///
/// In simulation, this will create new timelines for each choice.
///
/// An alternative implementation that is conceptually more elegant would be to spawn a task for
/// every option, then wait on a "select" operation to choose the first one that is ready. However,
/// that probably causes much higher memory usage, and wil not be practical with large choice sets.
#[derive(Debug)]
pub(crate) struct HilbertsEpsilon {
    choice: Option<usize>,
    universes: usize,
}

impl HilbertsEpsilon {
    #[inline]
    pub(crate) fn new(universes: usize) -> Self {
        Self {
            choice: None,
            universes,
        }
    }

    #[inline]
    pub(crate) fn choices(&self) -> Option<usize> {
        match self.choice {
            Some(_) => None,
            None => Some(self.universes),
        }
    }

    #[inline]
    pub(crate) fn choose(&mut self, choice: usize) {
        assert!(self.choice.is_none(), "logic error: choice already made");
        assert!(choice < self.universes);
        self.choice = Some(choice);
    }

    #[inline]
    pub(crate) fn must_get(&self) -> usize {
        self.choice.expect("logic error: choice not yet made")
    }
}
