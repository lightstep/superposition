# TODO(rw): Support state space pruning through symmetry tags.
# TODO(rw): Define a NonemptyIterator for use in defining label sets.
# TODO(rw): Remove the fairness parameter from the futures-intrusive Mutex.
# TODO(rw): Add native hilbert epsilon support to the futures Executor, for large choice sets. Index choices by the edge label, not by stepping the task. However, this may require the epsilon choice set to be finite. Maybe this is a good use of symmetry-based pruning.
# TODO(rw): Provide blessed implementations of cross-task communcation primitives. For example, an Arc implementation that correctly yields to the scheduler so that concurrency can be explored.
# TODO(rw): Integrate bumalo (or equivalent) into the Executor, and use that to minimize heap allocations for creating tasks.
