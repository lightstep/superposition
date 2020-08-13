//! Defines the Kripke structure.

/// An implicit [Kripke structure](https://en.wikipedia.org/wiki/Kripke_structure)
/// that defines a state space.
///
/// Unlike a traditional Kripke structure, this:
///   1. prohibits the existence of multiple start states,
///   2. permits the existence of infinite sets of states (path depth), and
///   3. permits the existence of infinite sets of successors (path width).
///
pub trait KripkeStructure {
    type Label: Copy;
    type LabelIterator: Iterator<Item = Self::Label> + Clone;

    /// Make a transition in the state space.
    ///
    /// The label can be assumed to come from the previous call to successors.
    fn transition(self, label: Self::Label);

    /// Return an iterator for the edge labels, or indicate that a trajectory is complete.
    ///
    /// If the result is Some, then the label iterator must be nonempty.
    ///
    /// If the result is None, then the trajectory is complete.
    fn successors(self) -> Option<Self::LabelIterator>;

    /// Restart the state space trajectory from the root state.
    fn restart(self);
}
