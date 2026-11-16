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
        outboxes_(graph.num_vertices()) {}

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

    // Only messages between adjacent vertices are allowed.
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

    outboxes_[sender].push_back({sender, recipient, message});
  }

    template <class F>
    void execute_round(F&& process_vertex) {
      // Phase 1: Every vertex processes its current inbox.
      graph_.parallel_for_vertices([&](size_t vertex) {
        process_vertex(static_cast<VertexId>(vertex), *this);
      });

      // Phase 2: Prepare the inboxes for the next round.
      for (auto& inbox : inboxes_) {
        inbox.clear();
      }

      // Phase 3: Deliver messages sent during this round.
      for (auto& outbox : outboxes_) {
        for (auto& message : outbox) {
          inboxes_[message.recipient].push_back(std::move(message));
        }

        outbox.clear();
      }
    }

 private:
  const Graph& graph_;

  std::vector<State> states_;

  std::vector<std::vector<MessageEnvelope>> inboxes_;
  std::vector<std::vector<MessageEnvelope>> outboxes_;
};

}  // namespace distributed
