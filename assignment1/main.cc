#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "gbbs/gbbs.h"

#include "assignment1/algorithm.h"
#include "framework/common/graph_adapter.h"
#include "framework/message_passing/simulator.h"

namespace assignment1 {

// ============================================================
// Configuration
// ============================================================

// Maximum number of communication rounds.
//
// Can be overridden using the MIS_MAX_ROUNDS environment
// variable.
//
// Example:
//   MIS_MAX_ROUNDS=500 bazel run //assignment1:mis -- ...
//
size_t read_max_rounds() {
  constexpr size_t default_max_rounds = 10000;

  const char* value = std::getenv("MIS_MAX_ROUNDS");

  if (value == nullptr) {
    return default_max_rounds;
  }

  const std::string text(value);

  if (text.empty() || text[0] == '-') {
    throw std::runtime_error(
        "MIS_MAX_ROUNDS must be a positive integer");
  }

  errno = 0;

  char* end = nullptr;

  const unsigned long long parsed =
      std::strtoull(value, &end, 10);

  if (errno == ERANGE ||
      end == value ||
      *end != '\0' ||
      parsed == 0 ||
      parsed > std::numeric_limits<size_t>::max()) {

    throw std::runtime_error(
        "MIS_MAX_ROUNDS must be a positive integer");
  }

  return static_cast<size_t>(parsed);
}

// ============================================================
// MIS verification
// ============================================================

template <class Graph>
size_t verify_mis(
    const distributed::GraphAdapter<Graph>& graph,
    const distributed::Simulator<
        distributed::GraphAdapter<Graph>,
        State,
        Message>& simulator) {

  using VertexId = gbbs::uintE;

  const size_t n = graph.num_vertices();

  std::vector<unsigned char> in_mis(n, 0);

  size_t mis_size = 0;

  // ----------------------------------------------------------
  // Check that every vertex has terminated and decided.
  // ----------------------------------------------------------

  for (size_t vertex = 0; vertex < n; ++vertex) {
    const VertexId id = static_cast<VertexId>(vertex);

    if (!simulator.is_terminated(id)) {
      throw std::runtime_error(
          "MIS verification failed: vertex did not terminate");
    }

    const Status status = simulator.state(id).status;

    if (status == Status::ACTIVE) {
      throw std::runtime_error(
          "MIS verification failed: undecided vertex");
    }

    if (status == Status::IN_MIS) {
      in_mis[vertex] = 1;
      ++mis_size;
    }
  }

  // ----------------------------------------------------------
  // Independence:
  //
  // No two adjacent vertices may belong to the MIS.
  // ----------------------------------------------------------

  for (size_t vertex = 0; vertex < n; ++vertex) {
    if (!in_mis[vertex]) {
      continue;
    }

    const VertexId id = static_cast<VertexId>(vertex);

    graph.for_each_neighbor(
        id,
        [&](VertexId neighbor) {
          if (in_mis[neighbor]) {
            throw std::runtime_error(
                "MIS verification failed: "
                "two adjacent vertices belong to the MIS");
          }
        });
  }

  // ----------------------------------------------------------
  // Maximality:
  //
  // Every vertex outside the MIS must have an MIS neighbor.
  // ----------------------------------------------------------

  for (size_t vertex = 0; vertex < n; ++vertex) {
    if (in_mis[vertex]) {
      continue;
    }

    const VertexId id = static_cast<VertexId>(vertex);

    bool has_mis_neighbor = false;

    graph.for_each_neighbor(
        id,
        [&](VertexId neighbor) {
          if (in_mis[neighbor]) {
            has_mis_neighbor = true;
          }
        });

    if (!has_mis_neighbor) {
      throw std::runtime_error(
          "MIS verification failed: "
          "a vertex outside the MIS has no MIS neighbor");
    }
  }

  std::cout << "MIS verification passed!\n";

  return mis_size;
}

// ============================================================
// Assignment execution
// ============================================================

template <class Graph>
double run_assignment(Graph& G, gbbs::commandLine P) {
  (void)P;

  using Adapter = distributed::GraphAdapter<Graph>;

  using Simulator =
      distributed::Simulator<Adapter, State, Message>;

  Adapter graph(G);

  Simulator simulator(
      graph,
      message_bits,
      distributed::CommunicationModel::CONGEST,
      0);

  const size_t max_rounds = read_max_rounds();

  std::cout << "Starting Assignment 1: Randomized MIS\n";

  std::cout << "  Vertices: "
            << graph.num_vertices() << "\n";

  std::cout << "  Maximum rounds: "
            << max_rounds << "\n";

  // ----------------------------------------------------------
  // Execute the student's distributed algorithm.
  // ----------------------------------------------------------

  try {
      simulator.run(
          [](auto& ctx) {
            assignment1::algorithm(ctx);
          },
          max_rounds);

  } catch (const std::exception& error) {

    std::cerr << "Simulation failed: "
              << error.what() << "\n";

    std::exit(EXIT_FAILURE);
  }

  // ----------------------------------------------------------
  // Verify the resulting MIS.
  // ----------------------------------------------------------

  const size_t mis_size = verify_mis(graph, simulator);

  // ----------------------------------------------------------
  // Print execution statistics.
  // ----------------------------------------------------------

  std::cout << "Randomized MIS completed!\n";

  std::cout << "  MIS size: "
            << mis_size << "\n";

  std::cout << "  Communication rounds: "
            << simulator.completed_rounds() << "\n";

  std::cout << "  Maximum message size: "
            << simulator.max_message_bits()
            << " bits\n";

  std::cout << "  Maximum bits per directed edge per round: "
            << simulator.max_edge_bits_per_round()
            << "\n";

  std::cout << "  Total communication: "
            << simulator.total_message_bits()
            << " bits\n";

  return 0.0;
}

}  // namespace assignment1

// GBBS benchmark entry point.
//
// The false argument selects the unweighted graph mode.
generate_main(assignment1::run_assignment, false);
