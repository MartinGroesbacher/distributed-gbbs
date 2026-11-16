#pragma once

#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

#include "framework/common/graph_adapter.h"

namespace distributed {

// Supported distributed communication models.
enum class CommunicationModel {
  LOCAL,
  CONGEST
};

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

  explicit MessagePassingRuntime(
      const Graph& graph,
      CommunicationModel model = CommunicationModel::CONGEST)
      : graph_(graph),
        model_(model),
        states_(graph.num_vertices()),
        inboxes_(graph.num_vertices()),
        outboxes_(graph.num_vertices()),
        terminated_(graph.num_vertices(), 0),
        completed_rounds_(0) {}

  size_t num_vertices() const {
    return graph_.num_vertices();
  }

  CommunicationModel model() const {
    return model_;
  }

  size_t completed_rounds() const {
    return completed_rounds_;
  }

  State& state(VertexId vertex) {
    return states_.at(vertex);
  }

  const std::vector<MessageEnvelope>& inbox(VertexId vertex) const {
    return inboxes_.at(vertex);
  }

  // Send one logical message to an adjacent vertex.
  void send(VertexId sender, VertexId recipient, const Message& message) {
    if (sender >= num_vertices() || recipient >= num_vertices()) {
      throw std::out_of_range("Invalid vertex ID");
    }

    if (is_terminated(sender)) {
      throw std::logic_error(
          "Terminated vertices cannot send messages");
    }

    // Check whether sender and recipient are adjacent.
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

    // Each vertex owns its outgoing message buffer.
    auto& outbox = outboxes_[sender];

    // Enforce at most one message per directed edge per round.
    for (const auto& existing : outbox) {
      if (existing.recipient == recipient) {
        throw std::logic_error(
            "Multiple messages to the same neighbor in one round");
      }
    }

    outbox.push_back({sender, recipient, message});
  }

  // Mark a vertex as terminated.
  void terminate(VertexId vertex) {
    terminated_.at(vertex) = 1;
  }

  bool is_terminated(VertexId vertex) const {
    return terminated_.at(vertex) != 0;
  }

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
    // Phase 1: Execute active vertices in parallel.
    graph_.parallel_for_vertices([&](size_t vertex) {
      const VertexId id = static_cast<VertexId>(vertex);

      if (!is_terminated(id)) {
        process_vertex(id, *this);
      }
    });

    // Phase 2: Remove messages from the previous round.
    for (auto& inbox : inboxes_) {
      inbox.clear();
    }

    // Phase 3: Deliver messages for the next round.
    for (auto& outbox : outboxes_) {
      for (auto& message : outbox) {
        if (!is_terminated(message.recipient)) {
          inboxes_[message.recipient].push_back(std::move(message));
        }
      }

      outbox.clear();
    }

    ++completed_rounds_;
  }

 private:
  const Graph& graph_;

  CommunicationModel model_;

  // Local state of each vertex.
  std::vector<State> states_;

  // Messages received in the current round.
  std::vector<std::vector<MessageEnvelope>> inboxes_;

  // Messages sent during the current round.
  // Each sender writes exclusively to its own outbox.
  std::vector<std::vector<MessageEnvelope>> outboxes_;

  // One independently writable termination flag per vertex.
  std::vector<unsigned char> terminated_;

  // Number of completed synchronous rounds.
  size_t completed_rounds_;
};

}  // namespace distributed
