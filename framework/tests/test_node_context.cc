#include <atomic>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "framework/common/graph_adapter.h"
#include "framework/message_passing/simulator.h"

namespace distributed {

struct TestState {
  size_t received = 0;
};

struct TestMessage {
  gbbs::uintE sender;
};

template <class Graph>
double test_node_context(Graph& G, gbbs::commandLine P) {
  GraphAdapter<Graph> adapter(G);

  // ============================================================
  // Test 1: Basic message passing through NodeContext
  // ============================================================

  Simulator<GraphAdapter<Graph>, TestState, TestMessage> simulator(adapter);

  // Round 1: Every vertex broadcasts its own ID.
  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Initial inbox is not empty");
    }

    ctx.broadcast(TestMessage{ctx.id()});
  });

  // Round 2: Verify received messages.
  simulator.execute_round([&](auto& ctx) {
    const auto& messages = ctx.messages();

    const size_t expected = G.get_vertex(ctx.id()).out_degree();

    if (messages.size() != expected) {
      throw std::runtime_error("Incorrect number of received messages");
    }

    for (const auto& message : messages) {
      if (message.payload.sender != message.sender) {
        throw std::runtime_error("Incorrect sender information");
      }
    }

    ctx.state().received = messages.size();
  });

  // Round 3: Old messages must have disappeared.
  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Old messages were not cleared");
    }
  });

  std::cout << "NodeContext verification passed!\n";

  // ============================================================
  // Test 2: Vertex termination
  // ============================================================

  if (adapter.num_vertices() == 0) {
    throw std::runtime_error("Termination test requires a nonempty graph");
  }

  Simulator<GraphAdapter<Graph>, TestState, TestMessage>
      termination_simulator(adapter);

  std::atomic<size_t> vertex_zero_executions{0};

  // Record vertex 0's neighbors.
  std::vector<gbbs::uintE> neighbors_of_zero;

  adapter.for_each_neighbor(0, [&](gbbs::uintE neighbor) {
    neighbors_of_zero.push_back(neighbor);
  });

  // Round 1: Vertex 0 broadcasts and terminates.
  termination_simulator.execute_round([&](auto& ctx) {
    if (ctx.id() == 0) {
      vertex_zero_executions.fetch_add(
          1, std::memory_order_relaxed);

      ctx.broadcast(TestMessage{0});
      ctx.terminate();
    }
  });

  // Round 2: Vertex 0 must not execute again.
  // Its neighbors should receive its final message.
  termination_simulator.execute_round([&](auto& ctx) {
    if (ctx.id() == 0) {
      vertex_zero_executions.fetch_add(
          1, std::memory_order_relaxed);
    }

    size_t expected = 0;

    for (auto neighbor : neighbors_of_zero) {
      if (neighbor == ctx.id()) {
        ++expected;
      }
    }

    if (ctx.messages().size() != expected) {
      throw std::runtime_error(
          "Incorrect message delivery after termination");
    }

    for (const auto& message : ctx.messages()) {
      if (message.sender != 0 ||
          message.payload.sender != 0) {
        throw std::runtime_error(
            "Incorrect final message from terminated vertex");
      }
    }
  });

  if (vertex_zero_executions.load() != 1) {
    throw std::runtime_error("Terminated vertex executed again");
  }

  if (!termination_simulator.is_terminated(0)) {
    throw std::runtime_error("Vertex 0 was not terminated");
  }

  // Round 3: Terminate all remaining vertices.
  termination_simulator.execute_round([](auto& ctx) {
    ctx.terminate();
  });

  if (!termination_simulator.all_terminated()) {
    throw std::runtime_error("Not all vertices terminated");
  }

  // Round 4: No vertex should execute anymore.
  std::atomic<size_t> executions_after_termination{0};

  termination_simulator.execute_round([&](auto& ctx) {
    executions_after_termination.fetch_add(
        1, std::memory_order_relaxed);
  });

  if (executions_after_termination.load() != 0) {
    throw std::runtime_error(
        "Vertices executed after global termination");
  }

  std::cout << "Vertex termination verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_node_context, false);
