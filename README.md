# Superposition

Verify concurrent behavior of real code using finite-space model checking.

## Usage

```toml
[dev-dependencies]
superposition = { git = "ssh://git@github.com/eradb/superposition.git", branch = "main" }
```

## Why finite-space model checking of real code?

The current known state-of-the-art of distributed database correctness testing is to implement randomized fault injection. This typically takes the form of fuzz testing database operations, by randomly causing simulated network timeouts, hardware errors, logic errors, and so on. While that approach may catch some bugs, it does not provide anything close to a proof of correctness.

To paraphrase Dijkstra, randomized testing can show the presence of bugs, but not their absence.

Nevertheless, randomized fault injection is so effective that Kyle Kingsbury has reportedly said that there's no point in running Jepsen tests against FoundationDB, because FoundationDB's own fuzz tests mitigate the need for a system like Jepsen.

But, what is beyond fuzzing? Can we find a testing methodology that acts like a proof of correctness? That can show the absence of bugs, not just their presence?

In my mind, we can achieve the next order of magnitude improvement in correctness by conducting exhaustive fault injection. The idea is to run our concurrent code in every possible timeline, and test correctness properties of each timeline.

For example, if we are given a distributed key-value store with three database servers and two clients, we would like to test that all possible concurrent executions of the clients making GET/SET operations on a single key are correct (e.g. no inconsistent reads).

One of the barriers to exhaustive correctness testing is that, as the number of concurrent processes increases, the combinatorial explosion of states causes the computational burden to increase super-exponentially. Testing a concurrent system with three servers and two clients may be feasible, but testing the same system with seven servers and ten clients may be infeasible.

There are strategies to reduce this computational burden. The most important runtime strategy is to identify symmetries in the state space, then prune them (a feature which is on the roadmap for this library).

However, more fundamentally, adhering to the Small Scope Hypothesis may be the most important way to mitigate the combinatorial explosion of states. The Small Scope Hypothesis says that if a program can fail, then it will fail on a simple case. (For more background on this, the paper "SmallCheck and Lazy SmallCheck" provides a good overview.)

Therefore, if we believe the Small Scope Hypothesis, and we exhaustively check our code in those small scopes, then we have reason to believe that our code is correct.

## Why Superposition?

Specification systems like TLC and Alloy provide exhaustive finite model checking for huge state spaces, but they don't work on real code. Instead, they work on domain-specific languages that are meant for specifying systems.

Simulation systems to test real code are either not exhaustive (they are just fuzzers), or they are not intended for testing billions and trillions of states (such as Carl Lerche's Loom project).

Superposition tries to get the benefits of both: exhaustively testing the state-space of real code.

## Features

Choice operator: Superposition provides tooling to test system behavior for each of a set of "choice" variables, analogous to Hilbert's Epsilon operator. For example, a choice variable could model a function that can return true or false. It does this by evaluating all possible states in which the function returns true, and all possible states in which the function returns false. This enables deterministic and exhaustive fault injection testing. (See the test suite for an example.)

Deterministic futures execution: Superposition's async Executor is deterministic, which guarantees correct evaluation of all possible states, even in the presence of restarts. This determinism is required to ensure that the state space traversals are exhaustive. It is also a useful building block for distributing the test runs over multiple threads or multiple servers. (See the test suite for an example.)

Uses real code: Superposition's async Simulator takes real code as input, and repeatedly runs it in different interleavings. The only special considerations are that 1) cross-task communication must be preceeded by a call to yield to the async scheduler, 2) no code can use real time, and instead must use simulated time, and 3) spawning an async task using the executor is not yet abstracted out (which would allow plugging in a different async executor for running your code in production). As a result, we can test the actual code that we write.

Reasonably-easy to use correctly: Superposition's separation of the KripkeStructure, async Executor and Controller, and depth-first search traversal of states means that most operations are typesafe, and it's straightforward to add new functionality. For example, in the near future, we may add symmetry pruning, distributed testing, breadth-first search of state spaces, and (deterministic) randomness.

Exact: The number of concurrent processes and the number of yield calls (to the async Executor) completely determines the number of state space trajectories that need to be explored. When all processes are identical, calculating this cardinality reduces to calculating the multinomial coefficient. (See the test suite for an example.)

Detects deadlocks: See the test suite for an example.

Detects livelocks: See the test suite for an example.
