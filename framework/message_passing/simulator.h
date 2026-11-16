#pragma once

#include <cstddef>
#include <utility>

#include "framework/message_passing/node_context.h"

namespace distributed {

template <class Graph, class State, class Message>
class Simulator {
 public:
  using Runtime = MessagePassingRuntime<Graph, State, Message>;
  using Context = NodeContext<Graph, State, Message>;
  using VertexId = gbbs::uintE;

  explicit Simulator(
      const Graph& graph,
      CommunicationModel model = CommunicationModel::CONGEST)
      : graph_(graph),
        runtime_(graph, model) {}

  // Execute one synchronous communication round.
  template <class F>
  void execute_round(F&& process_vertex) {
    runtime_.execute_round([&](VertexId vertex, Runtime& runtime) {
      Context ctx(vertex, runtime, graph_);
      process_vertex(ctx);
    });
  }

  // Return the selected communication model.
  CommunicationModel model() const {
    return runtime_.model();
  }

  // Number of completed synchronous rounds.
  size_t completed_rounds() const {
    return runtime_.completed_rounds();
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
