#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "framework/common/graph_adapter.h"
#include "framework/message_passing/message_size.h"
#include "framework/message_passing/runtime.h"

namespace distributed {

struct TestState {
  size_t messages_received = 0;
};

struct TestMessage {
  gbbs::uintE value;
};

template <class Graph>
double test_message_passing(Graph& G, gbbs::commandLine P) {
  GraphAdapter<Graph> adapter(G);

  auto message_bits = [](const TestMessage&, size_t n) {
    return vertex_id_bits(n);
  };

  MessagePassingRuntime<GraphAdapter<Graph>, TestState, TestMessage>
      runtime(adapter, message_bits);

  // Round 1: Every vertex sends its ID to all neighbors.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    if (!rt.inbox(vertex).empty()) {
      throw std::runtime_error("Inbox should initially be empty");
    }

    adapter.for_each_neighbor(vertex, [&](gbbs::uintE neighbor) {
      rt.send(vertex, neighbor, TestMessage{vertex});
    });
  });

  // Round 2: Verify senders and message contents.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    const auto& messages = rt.inbox(vertex);

    std::vector<gbbs::uintE> expected_senders;
    std::vector<gbbs::uintE> actual_senders;

    adapter.for_each_neighbor(vertex, [&](gbbs::uintE neighbor) {
      expected_senders.push_back(neighbor);
    });

    for (const auto& message : messages) {
      if (message.recipient != vertex ||
          message.payload.value != message.sender) {
        throw std::runtime_error("Incorrect message contents");
      }

      actual_senders.push_back(message.sender);
    }

    std::sort(expected_senders.begin(), expected_senders.end());
    std::sort(actual_senders.begin(), actual_senders.end());

    if (expected_senders != actual_senders) {
      throw std::runtime_error("Incorrect message senders");
    }

    rt.state(vertex).messages_received = messages.size();
  });

  // Round 3: No old messages should remain.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    if (!rt.inbox(vertex).empty()) {
      throw std::runtime_error("Old messages were not cleared");
    }
  });

  if (runtime.completed_rounds() != 3) {
    throw std::runtime_error("Incorrect round count");
  }

  const size_t expected_bits =
      G.m * vertex_id_bits(adapter.num_vertices());

  if (runtime.total_message_bits() != expected_bits) {
    throw std::runtime_error("Incorrect communication volume");
  }

  std::cout << "Message passing verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_message_passing, false);
