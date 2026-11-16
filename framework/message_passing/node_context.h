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

  // Return the total number of vertices.
  size_t num_nodes() const {
    return graph_.num_vertices();
  }

  // Access only this vertex's local state.
  State& state() {
    return runtime_.state(vertex_);
  }

  // Read messages received during the previous round.
  const std::vector<MessageEnvelope>& messages() const {
    return runtime_.inbox(vertex_);
  }

  // Send a message to a neighboring vertex.
  void send(VertexId neighbor, const Message& message) {
    runtime_.send(vertex_, neighbor, message);
  }

  // Send the same message to every neighbor.
  void broadcast(const Message& message) {
    graph_.for_each_neighbor(vertex_, [&](VertexId neighbor) {
      send(neighbor, message);
    });
  }

  // Iterate over the current vertex's neighbors.
  template <class F>
  void for_each_neighbor(F&& f) const {
    graph_.for_each_neighbor(vertex_, f);
  }

  // Terminate the current vertex.
  // It will no longer execute in subsequent rounds.
  void terminate() {
    runtime_.terminate(vertex_);
  }

 private:
  VertexId vertex_;
  Runtime& runtime_;
  const Graph& graph_;
};

}  // namespace distributed
