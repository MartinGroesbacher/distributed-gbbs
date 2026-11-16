# Assignment 1: Randomized Maximal Independent Set

## 1. Objective

Implement the randomized distributed **Maximal Independent Set (MIS)** algorithm discussed in the lecture notes. Your implementation will execute on a synchronous message-passing simulator built on top of the Graph-Based Benchmark Suite (GBBS).

You will implement the behavior of an individual vertex. The framework runs this behavior for all active vertices, synchronizes rounds and delivers messages. You must not solve the problem by directly accessing the entire graph or other vertices' local states.

## 2. Problem definition

For an undirected graph \(G=(V,E)\), a set \(U\subseteq V\) is **independent** if no edge has both endpoints in \(U\). It is **maximal** if no vertex can be added to \(U\) while preserving independence. Equivalently, every vertex outside \(U\) has at least one neighbor in \(U\).

Your implementation must compute a **maximal** independent set, not necessarily a *maximum* independent set.

## 3. Distributed execution model

Every vertex maintains its own local state and communicates with neighbors using the provided `NodeContext` API.

Execution proceeds in **synchronous rounds**. During a round, an active vertex can inspect its state and messages from the previous round, perform local computation, send messages, update its state and terminate. Messages sent in round \(t\) become available in round \(t+1\), **not** immediately. A terminated vertex is not scheduled again.

The simulator provides the following communication behavior:

- A vertex can send **at most one message to a particular neighbor per round** (one message per direction per edge).
- Messages are delivered at round boundaries.
- A message may only be sent to a neighboring vertex.
- The simulator measures logical message sizes and total transmitted payload bits.
- The simulator does **not** automatically fragment messages or enforce a fixed maximum number of bits per message.

### CONGEST and message sizes

In the theoretical CONGEST model, each directed edge carries at most \(O(\log n)\) bits per round, where \(n=|V|\). Our simulator records the encoded size reported by `message_bits()` rather than automatically proving or enforcing this asymptotic requirement.

**Fixed-width priorities, including 64-bit integer priorities, are permitted for this assignment.** The provided starter representation uses a one-bit message-type indicator and a 64-bit priority, yielding a logical size of 65 bits for a priority message. This is a practical finite-width representation; by itself, it is not a proof of an asymptotic \(O(\log n)\) bound for arbitrary graph sizes. Discuss the distinction and any limitations of your encoding in your report.

Sender and recipient IDs are available as framework envelope metadata and are not included in the logical payload-bit statistics. Do not assume the statistics include all transport overhead.

## 4. Algorithm to implement

Implement the randomized MIS algorithm presented in the lecture notes. Its main idea is to repeatedly let active vertices compete using random priorities:

1. Each active vertex chooses a random priority and announces it to its neighbors.
2. After receiving neighboring priorities, a vertex determines whether it has the smallest priority among its active neighbors.
3. A locally winning vertex joins the MIS, notifies its neighbors, and terminates.
4. A vertex notified that a neighbor joined the MIS terminates without joining.
5. Other active vertices continue to the next competition phase.

**Pay attention to synchronization.** Priority announcements and winner notifications cannot be received in the same round in which they are sent. Your implementation must use local state to manage the required phases correctly.

### Randomization and reproducibility

The theoretical algorithm chooses independent uniform random priorities from \([0,1)\). A practical implementation may use finite-width pseudorandom priorities. You must resolve equal priorities consistently, for example using vertex IDs as a tie-breaker.

Your implementation must support **reproducible experiments**. Document how its random seed is configured and record the seed used for each reported experiment. Maintain independent pseudorandom generator state for each vertex; do not use a shared mutable generator across parallel vertex executions. You may choose your own generator and seed-setting mechanism; no particular interface is prescribed by the framework.

Explain your generator, seeding strategy, and tie-breaking rule in your report. If you use finite-width priorities, discuss how they approximate the theoretical random choices.

## 5. Files and permitted modifications

The starter directory contains:

```text
assignment1/
├── BUILD
├── README.md
├── algorithm.h
├── algorithm.cpp
├── main.cc
├── run_tests.sh
└── test_graphs/  # Provided test graphs
```

**You may modify only `algorithm.h` and `algorithm.cpp` for the implementation.**

- `algorithm.h` defines the message representation, local vertex state and the templated `algorithm(Context& ctx)` function. Implement the distributed algorithm here. You may extend or replace the starter message and state definitions.
- `algorithm.cpp` implements `message_bits(const Message&, size_t n)`. Update it if you change the message representation. It must report the logical number of bits transmitted for each message type, rather than the in-memory `sizeof` of the C++ structure.

Do not modify `main.cc`, `BUILD`, `README.md`, or the `framework/` and `gbbs/` directories. The course staff will evaluate your solution with the provided infrastructure.

The starter `MessageType`, `Message`, and `State` declarations are **examples to build upon**, not mandatory final representations. The starter `algorithm()` function is intentionally incomplete: it compiles but does not terminate.

## 6. Student API: `NodeContext`

Your `algorithm(Context& ctx)` function is called once for each active vertex in each round. The supported operations are:

| Operation | Meaning |
|---|---|
| `ctx.id()` | ID of the current vertex. |
| `ctx.num_nodes()` | Number of vertices in the graph. |
| `ctx.state()` | Mutable reference to **this vertex's** persistent local state. |
| `ctx.messages()` | Read-only collection of messages delivered from the previous round. |
| `ctx.send(neighbor, message)` | Send one message to the specified neighbor for delivery next round. |
| `ctx.broadcast(message)` | Send the same message to every neighbor. |
| `ctx.for_each_neighbor(callback)` | Iterate over IDs of this vertex's neighbors. |
| `ctx.terminate()` | Permanently terminate the current vertex. |

Each received message is an envelope with `sender`, `recipient`, and `payload` fields. For example:

```cpp
for (const auto& envelope : ctx.messages()) {
    // envelope.sender identifies the neighbor.
    // envelope.payload contains the transmitted message.
}
```

To inspect neighbor IDs:

```cpp
ctx.for_each_neighbor([&](gbbs::uintE neighbor) {
    // Only the neighbor's ID is available here.
});
```

You cannot read another vertex's state. All information about other vertices must be obtained through the permitted interface and exchanged messages.

## 7. Implementation requirements

Your solution must:

1. Produce an **maximal independent set**.
2. Eventually terminate every vertex, with the appropriate probabilistic guarantees for a randomized algorithm.
3. Follow the randomized distributed algorithm from the lecture, rather than substituting a centralized or sequential MIS procedure.
4. Respect synchronous message delivery and the one-message-per-directed-edge-per-round rule.
5. Use only local state, neighbor IDs, and messages available through `NodeContext`.
6. Correctly classify each vertex as `IN_MIS` or `NOT_IN_MIS` before terminating, using the status definitions expected by the provided verifier.
7. Implement accurate logical payload-size accounting in `message_bits()`.
8. Support and document reproducible randomized experiments.

The simulator's successful completion and correctness checks on individual graphs do not constitute a general correctness proof.

## 8. Build and run

Run commands from the **repository root**.

Build the student algorithm library:

```bash
bazel build //assignment1:algorithm
```

Build the executable:

```bash
bazel build //assignment1:mis
```

Run on the provided symmetric GBBS graph:

```bash
bazel run //assignment1:mis -- \
    -s "$PWD/inputs/rMatGraph_J_5_100"
```

The assignment also includes eight test graphs in `assignment1/test_graphs/`: `empty_100`, `edge_2`, `path_100`, `cycle_100`, `star_100`, `complete_50`, `bipartite_20_20`, and `random_100`. To run your implementation on all of them, execute:

```bash
./assignment1/run_tests.sh
```

The script builds your implementation, runs it on each graph, and reports PASS/FAIL results and a final summary. You can also test a single graph, for example:

```bash
bazel run //assignment1:mis -- \
    -s "$PWD/assignment1/test_graphs/star_100"
```

The `-s` flag selects the symmetric graph-loading path. Use a valid path to an appropriate input graph.

The default simulation limit is 10,000 communication rounds. To change it:

```bash
MIS_MAX_ROUNDS=500 bazel run //assignment1:mis -- \
    -s "$PWD/inputs/rMatGraph_J_5_100"
```

The environment variable `MIS_MAX_ROUNDS` controls **only the round limit**, not the random seed. If your algorithm needs a configurable seed, implement and document that mechanism in your permitted files.

**Expected starter behavior:** The unfinished skeleton builds but fails with `Simulation did not terminate within the round limit`. This is expected until you implement the algorithm.

GBBS may execute the benchmark multiple times during one invocation. If you require identical randomized outcomes across repeated executions, initialize your local pseudorandom states reproducibly for each run.

## 9. Verification and statistics

After all vertices terminate, `main.cc` verifies that:

- Every vertex has terminated and has a final MIS status.
- No edge connects two vertices in the MIS (**independence**).
- Every vertex outside the MIS has a neighbor inside it (**maximality**).

For successful executions, the program reports the MIS size, number of communication rounds, maximum message size, maximum bits per directed edge per round and total transmitted payload bits.

Test multiple graph structures, sizes, and random seeds. The automatic verifier checks an individual execution, not the correctness or theoretical complexity of the algorithm for all possible inputs.

## 10. Report (`report.pdf`)

Submit a PDF named `report.pdf` inside `assignment1/`. Your report should contain:

### Algorithm and implementation

Describe your local states, communication phases, message types and decisions to join or leave the MIS. Explain how you implement the lecture algorithm.

### Correctness and termination

Argue why the result is independent and maximal. Explain why vertices eventually terminate and identify any probabilistic assumptions.

### Communication complexity

Analyze the number of synchronous communication rounds and the size of messages sent per directed edge per round. Explain how your representation relates to the theoretical \(O(\log n)\)-bit CONGEST requirement. Discuss any limitations of fixed-width priorities and the fact that the simulator measures, rather than proves, compliance.

### Randomization

Identify your pseudorandom generator, seeding strategy, tie-breaking rule and the method for reproducing a run.

### Experiments

Test graphs of different sizes and structures using multiple seeds. Report relevant results such as MIS size, communication rounds and message sizes. Include enough detail (input graph, seed, configuration) for someone else to reproduce your experiments, and compare observations with theoretical expectations.

## 11. Submission

Submit through your **public GitHub fork** of the course repository. Your completed submission must include:

```text
assignment1/
├── algorithm.h
├── algorithm.cpp
└── report.pdf
```

The other starter files remain present and unchanged. Commit and push your work before the deadline. Course staff will collect the repository revision associated with the submission deadline and test it locally.

**The submission deadline and detailed grading criteria will be announced separately.**

## 12. Academic integrity

External resources and AI-assisted programming tools are permitted. You must nevertheless understand your solution and be able to explain its behavior and analysis. We may conduct short oral checks. Plagiarism or unauthorized copying between submissions is prohibited and may result in zero points for the students involved.

The goal is not merely to obtain a valid MIS: it is to demonstrate an understanding of **distributed randomized computation** through correct local decisions, synchronous communication, and a reasoned complexity analysis.
