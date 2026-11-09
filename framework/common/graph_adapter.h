#pragma once

#include <cstddef>

#include "gbbs/gbbs.h"

namespace distributed {

template <class Graph>
class GraphAdapter {
 public:
  explicit GraphAdapter(const Graph& graph)
      : graph_(graph) {}

  size_t num_vertices() const {
    return graph_.n;
  }

  template <class F>
  void parallel_for_vertices(F&& f) const {
    gbbs::parallel_for(0, graph_.n, [&](size_t i) {
      f(i);
    });
  }

  template <class F>
  void for_each_neighbor(gbbs::uintE vertex, F&& f) const {
    auto callback = [&](gbbs::uintE source,
                        gbbs::uintE destination,
                        auto weight) {
      f(destination);
    };

    graph_.get_vertex(vertex).out_neighbors().map(callback, false);
  }

 private:
  const Graph& graph_;
};

}  // namespace distributed