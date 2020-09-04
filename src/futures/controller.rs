//! Defines the Controller trait that provides user hooks.

use super::executor::{ChoiceTaken, Executor, Spawner};

/// Events used to track behavior and manage state during a simulation.
///
/// The primary purpose of this trait is for setting up and tearing down user-provided async code,
/// so that the code can be simulated repeatedly. This is also where allocations in a simulation
/// can be reused.
pub trait Controller {
    /// Triggers on every state transition. The provided ChoiceTaken can be destructured to track
    /// which state transitions occurred and why.
    ///
    /// # Example uses:
    ///
    /// - Storing state-specific metadata in a Vec, to be analyzed at the end of a trajectory.
    /// - Checking state-specific invariants.
    fn on_transition(&mut self, choice_taken: ChoiceTaken);

    /// Triggers when the successor set is empty.
    ///
    /// Example uses:
    ///
    /// - Checking an entire history of state metadata for correctness.
    /// - Accumulating counts of the number of trajectories evaluated.
    fn on_end_of_trajectory(&mut self, ex: &Executor);

    /// Triggers when a trajectory is reset.
    ///
    /// # Example uses:
    ///
    /// - Spawning async tasks.
    /// - Resetting variables that hold data used by the simulation.
    fn on_restart(&mut self, spawner: &Spawner);
}
