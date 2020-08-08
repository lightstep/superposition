//! Defines the Controller trait that provides user hooks.

use super::executor::Executor;

/// Events used to track behavior and manage state during a simulation.
///
/// The primary purpose of this trait is for setting up and tearing down user-provided async code,
/// so that the code can be run repeatedly.
///
/// Each method should move `self`.
/// This is to provide flexibility and efficiency for managing state, such as for a local allocator.
pub trait Controller {
    /// Triggers on every state transition.
    fn on_transition(self) -> Self;

    /// Triggers when the successor set is empty.
    fn on_end_of_trajectory(self, ex: &Executor) -> Self;

    /// Triggers when a trajectory is reset.
    fn on_restart(self, ex: &Executor) -> Self;
}
