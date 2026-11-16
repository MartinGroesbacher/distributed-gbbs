#pragma once

#include <cstddef>
#include <stdexcept>
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

  // Execute exactly one synchronous communication round.
  template <class F>
  void execute_round(F&& process_vertex) {
    runtime_.execute_round([&](VertexId vertex, Runtime& runtime) {
      Context ctx(vertex, runtime, graph_);
      process_vertex(ctx);
    });
  }

  // Execute rounds until all vertices have terminated.
  //
  // max_rounds limits the number of additional rounds executed
  // by this call, not the lifetime total.
  //
  // Throws if the limit is exhausted while active vertices remain.
  template <class F>
  void run(F&& process_vertex, size_t max_rounds) {
    size_t rounds_executed = 0;

    while (!all_terminated() && rounds_executed < max_rounds) {
      execute_round(process_vertex);
      ++rounds_executed;
    }

    if (!all_terminated()) {
      throw std::runtime_error(
          "Simulation did not terminate within the round limit");
    }
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

  // Read-only access to a vertex's final state.
  //
  // Intended for testing and verification after simulation.
  // Student algorithms should use ctx.state() instead.
  const State& state(VertexId vertex) const {
    return static_cast<const Runtime&>(runtime_).state(vertex);
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
