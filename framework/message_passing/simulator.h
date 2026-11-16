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
  using MessageSizeFunction = typename Runtime::MessageSizeFunction;

  explicit Simulator(
      const Graph& graph,
      MessageSizeFunction message_size,
      CommunicationModel model = CommunicationModel::CONGEST,
      size_t reference_constant = 0)
      : graph_(graph),
        runtime_(
            graph,
            std::move(message_size),
            model,
            reference_constant) {}

  template <class F>
  void execute_round(F&& process_vertex) {
    runtime_.execute_round([&](VertexId vertex, Runtime& runtime) {
      Context ctx(vertex, runtime, graph_);
      process_vertex(ctx);
    });
  }

  CommunicationModel model() const {
    return runtime_.model();
  }

  size_t completed_rounds() const {
    return runtime_.completed_rounds();
  }

  size_t max_message_bits() const {
    return runtime_.max_message_bits();
  }

  size_t max_edge_bits_per_round() const {
    return runtime_.max_edge_bits_per_round();
  }

  size_t total_message_bits() const {
    return runtime_.total_message_bits();
  }

  size_t reference_bandwidth_bits() const {
    return runtime_.reference_bandwidth_bits();
  }

  bool reference_bandwidth_exceeded() const {
    return runtime_.reference_bandwidth_exceeded();
  }

  bool is_terminated(VertexId vertex) const {
    return runtime_.is_terminated(vertex);
  }

  bool all_terminated() const {
    return runtime_.all_terminated();
  }

 private:
  const Graph& graph_;
  Runtime runtime_;
};

}  // namespace distributed
