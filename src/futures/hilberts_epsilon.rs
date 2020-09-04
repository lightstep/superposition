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
    chosen_universe: Option<usize>,
    universes: usize,
    id: HilbertsEpsilonId,
}

impl HilbertsEpsilon {
    #[inline]
    pub(crate) fn new(id: HilbertsEpsilonId, universes: usize) -> Self {
        Self {
            chosen_universe: None,
            universes,
            id,
        }
    }

    #[inline]
    pub(crate) fn id(&self) -> HilbertsEpsilonId {
        self.id
    }

    #[inline]
    pub(crate) fn choices(&self) -> Option<usize> {
        match self.chosen_universe {
            Some(_) => None,
            None => Some(self.universes),
        }
    }

    #[inline]
    pub(crate) fn choose(&mut self, choice: usize) {
        assert!(
            self.chosen_universe.is_none(),
            "logic error: choice already made"
        );
        assert!(choice < self.universes);
        self.chosen_universe = Some(choice);
    }

    #[inline]
    pub(crate) fn chosen_universe(&self) -> Option<usize> {
        self.chosen_universe
    }

    #[inline]
    pub(crate) fn must_get_chosen_universe(&self) -> usize {
        self.chosen_universe()
            .expect("logic error: universe choice not yet made")
    }
}

/// A unique identifier assigned to each hilberts epsilon upon its creation.
///
/// Used for ordering hilberts epsilons, so that execution is deterministic.
///
/// TODO(rw): Convert this to a struct type.
pub type HilbertsEpsilonId = usize;
