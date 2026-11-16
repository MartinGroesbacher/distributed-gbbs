#include <iostream>
#include <stdexcept>

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

  Simulator<GraphAdapter<Graph>, TestState, TestMessage> simulator(adapter);

  // Round 1: Every vertex broadcasts its own ID.
  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Initial inbox is not empty");
    }

    ctx.broadcast(TestMessage{ctx.id()});
  });

  // Round 2: Every vertex checks its received messages.
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

  // Round 3: Messages from round 1 must have disappeared.
  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Old messages were not cleared");
    }
  });

  std::cout << "NodeContext verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_node_context, false);
