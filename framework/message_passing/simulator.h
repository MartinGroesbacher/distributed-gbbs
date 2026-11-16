#pragma once

#include <utility>

#include "framework/message_passing/node_context.h"

namespace distributed {

template <class Graph, class State, class Message>
class Simulator {
 public:
  using Runtime = MessagePassingRuntime<Graph, State, Message>;
  using Context = NodeContext<Graph, State, Message>;
  using VertexId = gbbs::uintE;

  explicit Simulator(const Graph& graph)
      : graph_(graph), runtime_(graph) {}

  // Execute one synchronous communication round.
  template <class F>
  void execute_round(F&& process_vertex) {
    runtime_.execute_round([&](VertexId vertex, Runtime& runtime) {
      Context ctx(vertex, runtime, graph_);
      process_vertex(ctx);
    });
  }

  // Check whether a particular vertex has terminated.
  bool is_terminated(VertexId vertex) const {
    return runtime_.is_terminated(vertex);
  }

  // Check whether every vertex has terminated.
  bool all_terminated() const {
    return runtime_.all_terminated();
  }

 private:
  const Graph& graph_;
  Runtime runtime_;
};

}  // namespace distributed
