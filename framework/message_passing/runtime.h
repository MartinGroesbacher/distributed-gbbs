#pragma once

#include <cstddef>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "framework/common/graph_adapter.h"
#include "framework/message_passing/message_size.h"

namespace distributed {

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

  // Calculates the logical encoded size of a message in bits.
  using MessageSizeFunction =
      std::function<size_t(const Message&, size_t)>;

  explicit MessagePassingRuntime(
      const Graph& graph,
      MessageSizeFunction message_size,
      CommunicationModel model = CommunicationModel::CONGEST,
      size_t reference_constant = 0)
      : graph_(graph),
        model_(model),
        message_size_(std::move(message_size)),
        states_(graph.num_vertices()),
        inboxes_(graph.num_vertices()),
        outboxes_(graph.num_vertices()),
        terminated_(graph.num_vertices(), 0),
        completed_rounds_(0),
        max_message_bits_(0),
        max_edge_bits_per_round_(0),
        total_message_bits_(0),
        reference_bandwidth_bits_(
            model == CommunicationModel::CONGEST
                ? distributed::reference_bandwidth_bits(
                      graph.num_vertices(), reference_constant)
                : 0),
        reference_bandwidth_exceeded_(false) {
    if (!message_size_) {
      throw std::invalid_argument(
          "A message-size function must be provided");
    }
  }

  size_t num_vertices() const {
    return graph_.num_vertices();
  }

  CommunicationModel model() const {
    return model_;
  }

  size_t completed_rounds() const {
    return completed_rounds_;
  }

  size_t max_message_bits() const {
    return max_message_bits_;
  }

  size_t max_edge_bits_per_round() const {
    return max_edge_bits_per_round_;
  }

  size_t total_message_bits() const {
    return total_message_bits_;
  }

  size_t reference_bandwidth_bits() const {
    return reference_bandwidth_bits_;
  }

  bool reference_bandwidth_exceeded() const {
    return reference_bandwidth_exceeded_;
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

    auto& outbox = outboxes_[sender];

    // At most one logical message per directed edge per round.
    for (const auto& existing : outbox) {
      if (existing.recipient == recipient) {
        throw std::logic_error(
            "Multiple messages to the same neighbor in one round");
      }
    }

    outbox.push_back({sender, recipient, message});
  }

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

    // Phase 2: Measure outgoing communication.
    //
    // This runs after the parallel phase, so no worker
    // is still modifying an outbox.
    //
    // Statistics include messages sent to vertices that
    // terminate during this round, even if those messages
    // are subsequently discarded.

    size_t round_max_message_bits = 0;
    size_t round_total_bits = 0;

    for (const auto& outbox : outboxes_) {
      for (const auto& message : outbox) {
        const size_t bits =
            message_size_(message.payload, num_vertices());

        if (bits > round_max_message_bits) {
          round_max_message_bits = bits;
        }

        if (bits > std::numeric_limits<size_t>::max()
                       - round_total_bits) {
          throw std::overflow_error(
              "Round communication volume overflow");
        }

        round_total_bits += bits;
      }
    }

    if (round_total_bits >
        std::numeric_limits<size_t>::max()
            - total_message_bits_) {
      throw std::overflow_error(
          "Total communication volume overflow");
    }

    // Phase 3: Update statistics.
    //
    // Because we enforce one message per directed edge
    // per round, maximum edge load equals maximum
    // individual message size.

    if (round_max_message_bits > max_message_bits_) {
      max_message_bits_ = round_max_message_bits;
    }

    if (round_max_message_bits > max_edge_bits_per_round_) {
      max_edge_bits_per_round_ = round_max_message_bits;
    }

    total_message_bits_ += round_total_bits;

    if (model_ == CommunicationModel::CONGEST &&
        reference_bandwidth_bits_ > 0 &&
        round_max_message_bits > reference_bandwidth_bits_) {
      reference_bandwidth_exceeded_ = true;
    }

    // Phase 4: Remove messages from the previous round.
    for (auto& inbox : inboxes_) {
      inbox.clear();
    }

    // Phase 5: Deliver messages for the next round.
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
  MessageSizeFunction message_size_;

  std::vector<State> states_;
  std::vector<std::vector<MessageEnvelope>> inboxes_;
  std::vector<std::vector<MessageEnvelope>> outboxes_;
  std::vector<unsigned char> terminated_;

  size_t completed_rounds_;

  // Communication statistics.
  size_t max_message_bits_;
  size_t max_edge_bits_per_round_;
  size_t total_message_bits_;

  // Optional CONGEST diagnostic.
  size_t reference_bandwidth_bits_;
  bool reference_bandwidth_exceeded_;
};

}  // namespace distributed
