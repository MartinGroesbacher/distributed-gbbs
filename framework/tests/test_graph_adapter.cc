#include <atomic>
#include <iostream>
#include <stdexcept>

#include "framework/common/graph_adapter.h"

namespace distributed {

template <class Graph>
double test_graph_adapter(Graph& G, gbbs::commandLine P) {
  GraphAdapter<Graph> adapter(G);

  const size_t n = adapter.num_vertices();

  std::atomic<size_t> total_edges{0};
  std::atomic<size_t> invalid_neighbors{0};
  std::atomic<size_t> degree_mismatches{0};

  adapter.parallel_for_vertices([&](size_t vertex) {
    size_t degree = 0;

    adapter.for_each_neighbor(
        static_cast<gbbs::uintE>(vertex),
        [&](gbbs::uintE neighbor) {
          ++degree;

          if (neighbor >= n) {
            invalid_neighbors.fetch_add(1, std::memory_order_relaxed);
          }
        });

    total_edges.fetch_add(degree, std::memory_order_relaxed);

    if (degree != G.get_vertex(vertex).out_degree()) {
      degree_mismatches.fetch_add(1, std::memory_order_relaxed);
    }
  });

  // These checks remain active in optimized builds.
  if (n != G.n ||
      total_edges.load() != G.m ||
      invalid_neighbors.load() != 0 ||
      degree_mismatches.load() != 0) {
    throw std::runtime_error("Graph adapter verification failed!");
  }

  std::cout << "Vertices: " << n << "\n";
  std::cout << "Edges: " << total_edges.load() << "\n";
  std::cout << "Graph adapter verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_graph_adapter, false);