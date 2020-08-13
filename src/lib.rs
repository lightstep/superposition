//! Verifies concurrent behavior of real code using finite-space model checking.

pub mod dfs;
pub mod futures;

mod kripke_structure;
pub use kripke_structure::KripkeStructure;
