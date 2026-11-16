#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "framework/common/graph_adapter.h"
#include "framework/message_passing/runtime.h"

namespace distributed {

struct TestState {
  size_t messages_received = 0;
};

struct TestMessage {
  int value;
};

template <class Graph>
double test_message_passing(Graph& G, gbbs::commandLine P) {
  GraphAdapter<Graph> adapter(G);

  MessagePassingRuntime<GraphAdapter<Graph>, TestState, TestMessage>
      runtime(adapter);

  // Round 1: Every vertex sends its ID to all neighbors.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    if (!rt.inbox(vertex).empty()) {
      throw std::runtime_error("Inbox should initially be empty");
    }

    adapter.for_each_neighbor(vertex, [&](gbbs::uintE neighbor) {
      rt.send(vertex, neighbor, TestMessage{static_cast<int>(vertex)});
    });
  });

  // Round 2: Verify that each vertex receives exactly
  // one message from every neighbor.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    const auto& messages = rt.inbox(vertex);

    std::vector<gbbs::uintE> expected_senders;
    std::vector<gbbs::uintE> actual_senders;

    adapter.for_each_neighbor(vertex, [&](gbbs::uintE neighbor) {
      expected_senders.push_back(neighbor);
    });

    for (const auto& message : messages) {
      if (message.recipient != vertex ||
          message.payload.value != static_cast<int>(message.sender)) {
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

  // Round 3: No messages should remain.
  runtime.execute_round([&](gbbs::uintE vertex, auto& rt) {
    if (!rt.inbox(vertex).empty()) {
      throw std::runtime_error("Old messages were not cleared");
    }
  });

  std::cout << "Message passing verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_message_passing, false);
