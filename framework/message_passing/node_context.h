#pragma once

#include <cstddef>
#include <vector>

#include "framework/message_passing/runtime.h"

namespace distributed {

template <class Graph, class State, class Message>
class NodeContext {
 public:
  using VertexId = gbbs::uintE;
  using Runtime = MessagePassingRuntime<Graph, State, Message>;
  using MessageEnvelope = Envelope<Message>;

  NodeContext(VertexId vertex, Runtime& runtime, const Graph& graph)
      : vertex_(vertex), runtime_(runtime), graph_(graph) {}

  // Return the ID of the current vertex.
  VertexId id() const {
    return vertex_;
  }

  // Return the number of vertices in the graph.
  size_t num_nodes() const {
    return graph_.num_vertices();
  }

  // Access the current vertex's local state.
  State& state() {
    return runtime_.state(vertex_);
  }

  // Read messages received in the current round.
  const std::vector<MessageEnvelope>& messages() const {
    return runtime_.inbox(vertex_);
  }

  // Send a message to one neighboring vertex.
  void send(VertexId neighbor, const Message& message) {
    runtime_.send(vertex_, neighbor, message);
  }

  // Send the same message to all neighbors.
  void broadcast(const Message& message) {
    graph_.for_each_neighbor(vertex_, [&](VertexId neighbor) {
      send(neighbor, message);
    });
  }

  // Execute a callback for each neighboring vertex.
  template <class F>
  void for_each_neighbor(F&& f) const {
    graph_.for_each_neighbor(vertex_, f);
  }

 private:
  VertexId vertex_;
  Runtime& runtime_;
  const Graph& graph_;
};

}  // namespace distributed
