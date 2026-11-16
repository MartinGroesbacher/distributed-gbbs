#pragma once

#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

#include "framework/common/graph_adapter.h"

namespace distributed {

template <class Message>
struct Envelope {
  gbbs::uintE sender;
  gbbs::uintE recipient;
  Message payload;
};

template <class Graph, class State, class Message>
class MessagePassingRuntime {
 public:
  using VertexId = gbbs::uintE;
  using MessageEnvelope = Envelope<Message>;

  explicit MessagePassingRuntime(const Graph& graph)
      : graph_(graph),
        states_(graph.num_vertices()),
        inboxes_(graph.num_vertices()),
        outboxes_(graph.num_vertices()),
        terminated_(graph.num_vertices(), 0) {}

  size_t num_vertices() const {
    return graph_.num_vertices();
  }

  State& state(VertexId vertex) {
    return states_.at(vertex);
  }

  const std::vector<MessageEnvelope>& inbox(VertexId vertex) const {
    return inboxes_.at(vertex);
  }

  void send(VertexId sender, VertexId recipient, const Message& message) {
    if (sender >= num_vertices() || recipient >= num_vertices()) {
      throw std::out_of_range("Invalid vertex ID");
    }

    // Only communication between adjacent vertices is allowed.
    bool adjacent = false;

    graph_.for_each_neighbor(sender, [&](VertexId neighbor) {
      if (neighbor == recipient) {
        adjacent = true;
      }
    });

    if (!adjacent) {
      throw std::invalid_argument(
          "Cannot send a message to a non-neighbor");
    }

    // Each vertex writes only to its own outbox.
    outboxes_[sender].push_back({sender, recipient, message});
  }

  // Mark a vertex as terminated.
  void terminate(VertexId vertex) {
    terminated_.at(vertex) = 1;
  }

  // Check whether a vertex has terminated.
  bool is_terminated(VertexId vertex) const {
    return terminated_.at(vertex) != 0;
  }

  // Check whether all vertices have terminated.
  bool all_terminated() const {
    for (auto flag : terminated_) {
      if (flag == 0) {
        return false;
      }
    }

    return true;
  }

  template <class F>
  void execute_round(F&& process_vertex) {
    // Phase 1: Execute all active vertices in parallel.
    graph_.parallel_for_vertices([&](size_t vertex) {
      const VertexId id = static_cast<VertexId>(vertex);

      if (!is_terminated(id)) {
        process_vertex(id, *this);
      }
    });

    // Phase 2: Clear messages from the previous round.
    for (auto& inbox : inboxes_) {
      inbox.clear();
    }

    // Phase 3: Deliver messages sent during this round.
    for (auto& outbox : outboxes_) {
      for (auto& message : outbox) {
        // Terminated vertices no longer receive messages.
        if (!is_terminated(message.recipient)) {
          inboxes_[message.recipient].push_back(std::move(message));
        }
      }

      outbox.clear();
    }
  }

 private:
  const Graph& graph_;

  // Local state of each vertex.
  std::vector<State> states_;

  // Messages received in the current round.
  std::vector<std::vector<MessageEnvelope>> inboxes_;

  // Messages sent during the current round.
  std::vector<std::vector<MessageEnvelope>> outboxes_;

  // One independently writable termination flag per vertex.
  // Avoid std::vector<bool> because of its packed-bit representation.
  std::vector<unsigned char> terminated_;
};

}  // namespace distributed
