#include <atomic>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "framework/common/graph_adapter.h"
#include "framework/message_passing/message_size.h"
#include "framework/message_passing/simulator.h"

namespace distributed {

struct TestState {
  size_t received = 0;
  size_t counter = 0;
};

struct TestMessage {
  gbbs::uintE sender;
  size_t extra_bits = 0;
};

size_t test_message_bits(const TestMessage& message, size_t n) {
  return vertex_id_bits(n) + message.extra_bits;
}

template <class Graph>
double test_node_context(Graph& G, gbbs::commandLine P) {
  GraphAdapter<Graph> adapter(G);

  const size_t n = adapter.num_vertices();
  const size_t id_bits = vertex_id_bits(n);


  Simulator<GraphAdapter<Graph>, TestState, TestMessage> simulator(
      adapter, test_message_bits, CommunicationModel::CONGEST);

  if (simulator.model() != CommunicationModel::CONGEST) {
    throw std::runtime_error("Incorrect communication model");
  }

  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Initial inbox is not empty");
    }

    ctx.broadcast(TestMessage{ctx.id()});
  });

  simulator.execute_round([&](auto& ctx) {
    const auto& messages = ctx.messages();

    const size_t expected = G.get_vertex(ctx.id()).out_degree();

    if (messages.size() != expected) {
      throw std::runtime_error("Incorrect number of received messages");
    }

    for (const auto& message : messages) {
      if (message.payload.sender != message.sender) {
        throw std::runtime_error("Incorrect sender information");
      }
    }

    ctx.state().received = messages.size();
  });

  simulator.execute_round([](auto& ctx) {
    if (!ctx.messages().empty()) {
      throw std::runtime_error("Old messages were not cleared");
    }
  });

  if (simulator.completed_rounds() != 3) {
    throw std::runtime_error("Incorrect round count");
  }

  if (G.m > 0 && simulator.max_message_bits() != id_bits) {
    throw std::runtime_error("Incorrect maximum message size");
  }

  if (simulator.total_message_bits() != G.m * id_bits) {
    throw std::runtime_error("Incorrect total communication volume");
  }

  std::cout << "NodeContext verification passed!\n";

  if (n == 0) {
    throw std::runtime_error("Termination test requires a nonempty graph");
  }

  Simulator<GraphAdapter<Graph>, TestState, TestMessage>
      termination_simulator(adapter, test_message_bits);

  std::atomic<size_t> vertex_zero_executions{0};

  std::vector<gbbs::uintE> neighbors_of_zero;

  adapter.for_each_neighbor(0, [&](gbbs::uintE neighbor) {
    neighbors_of_zero.push_back(neighbor);
  });

  termination_simulator.execute_round([&](auto& ctx) {
    if (ctx.id() == 0) {
      vertex_zero_executions.fetch_add(
          1, std::memory_order_relaxed);

      ctx.broadcast(TestMessage{0});
      ctx.terminate();
    }
  });

  termination_simulator.execute_round([&](auto& ctx) {
    if (ctx.id() == 0) {
      vertex_zero_executions.fetch_add(
          1, std::memory_order_relaxed);
    }

    size_t expected = 0;

    for (auto neighbor : neighbors_of_zero) {
      if (neighbor == ctx.id()) {
        ++expected;
      }
    }

    if (ctx.messages().size() != expected) {
      throw std::runtime_error(
          "Incorrect message delivery after termination");
    }

    for (const auto& message : ctx.messages()) {
      if (message.sender != 0 ||
          message.payload.sender != 0) {
        throw std::runtime_error(
            "Incorrect final message from terminated vertex");
      }
    }
  });

  if (vertex_zero_executions.load() != 1) {
    throw std::runtime_error("Terminated vertex executed again");
  }

  if (!termination_simulator.is_terminated(0)) {
    throw std::runtime_error("Vertex 0 was not terminated");
  }

  termination_simulator.execute_round([](auto& ctx) {
    ctx.terminate();
  });

  if (!termination_simulator.all_terminated()) {
    throw std::runtime_error("Not all vertices terminated");
  }

  std::atomic<size_t> executions_after_termination{0};

  termination_simulator.execute_round([&](auto& ctx) {
    executions_after_termination.fetch_add(
        1, std::memory_order_relaxed);
  });

  if (executions_after_termination.load() != 0) {
    throw std::runtime_error(
        "Vertices executed after global termination");
  }

  if (termination_simulator.completed_rounds() != 4) {
    throw std::runtime_error("Incorrect termination round count");
  }

  std::cout << "Vertex termination verification passed!\n";

  for (CommunicationModel model :
       {CommunicationModel::LOCAL, CommunicationModel::CONGEST}) {

    Simulator<GraphAdapter<Graph>, TestState, TestMessage>
        duplicate_simulator(adapter, test_message_bits, model);

    if (duplicate_simulator.model() != model) {
      throw std::runtime_error("Communication model mismatch");
    }

    std::atomic<size_t> duplicates_rejected{0};
    std::atomic<size_t> messages_sent{0};

    duplicate_simulator.execute_round([&](auto& ctx) {
      bool has_neighbor = false;
      gbbs::uintE first_neighbor = 0;

      ctx.for_each_neighbor([&](gbbs::uintE neighbor) {
        if (!has_neighbor) {
          has_neighbor = true;
          first_neighbor = neighbor;
        }

        ctx.send(neighbor, TestMessage{ctx.id()});

        messages_sent.fetch_add(
            1, std::memory_order_relaxed);
      });

      if (has_neighbor) {
        try {
          ctx.send(first_neighbor, TestMessage{ctx.id()});
        } catch (const std::logic_error&) {
          duplicates_rejected.fetch_add(
              1, std::memory_order_relaxed);
        }
      }
    });

    size_t expected_rejections = 0;

    for (size_t vertex = 0; vertex < n; ++vertex) {
      bool has_neighbor = false;

      adapter.for_each_neighbor(
          static_cast<gbbs::uintE>(vertex),
          [&](gbbs::uintE neighbor) {
            has_neighbor = true;
          });

      if (has_neighbor) {
        ++expected_rejections;
      }
    }

    if (duplicates_rejected.load() != expected_rejections) {
      throw std::runtime_error(
          "Duplicate message restriction failed");
    }

    if (messages_sent.load() != G.m) {
      throw std::runtime_error(
          "Incorrect number of successful sends");
    }

    duplicate_simulator.execute_round([&](auto& ctx) {
      const size_t expected =
          G.get_vertex(ctx.id()).out_degree();

      if (ctx.messages().size() != expected) {
        throw std::runtime_error(
            "Incorrect message count after duplicate rejection");
      }
    });

    if (duplicate_simulator.completed_rounds() != 2) {
      throw std::runtime_error(
          "Incorrect duplicate-test round count");
    }

    if (duplicate_simulator.total_message_bits() != G.m * id_bits) {
      throw std::runtime_error(
          "Incorrect communication volume after duplicate rejection");
    }

    if (model == CommunicationModel::LOCAL) {
      std::cout << "LOCAL message restriction passed!\n";
    } else {
      std::cout << "CONGEST message restriction passed!\n";
    }
  }

  const size_t reference_constant = 2;
  const size_t reference_bits =
      reference_bandwidth_bits(n, reference_constant);

  Simulator<GraphAdapter<Graph>, TestState, TestMessage>
      statistics_simulator(
          adapter,
          test_message_bits,
          CommunicationModel::CONGEST,
          reference_constant);

  // Round 1: Vertex 0 sends larger messages.
  statistics_simulator.execute_round([](auto& ctx) {
    const size_t extra_bits = (ctx.id() == 0) ? 100 : 0;

    ctx.broadcast(TestMessage{ctx.id(), extra_bits});
  });

  const size_t expected_max =
      neighbors_of_zero.empty() ? id_bits : id_bits + 100;

  if (G.m > 0 &&
      statistics_simulator.max_message_bits() != expected_max) {
    throw std::runtime_error(
        "Incorrect maximum message size");
  }

  if (G.m > 0 &&
      statistics_simulator.max_edge_bits_per_round() != expected_max) {
    throw std::runtime_error(
        "Incorrect maximum edge communication");
  }

  const size_t expected_total =
      G.m * id_bits + neighbors_of_zero.size() * 100;

  if (statistics_simulator.total_message_bits() != expected_total) {
    throw std::runtime_error(
        "Incorrect total communication volume");
  }

  if (statistics_simulator.reference_bandwidth_bits() !=
      reference_bits) {
    throw std::runtime_error(
        "Incorrect reference bandwidth");
  }

  const bool expected_warning =
      G.m > 0 && expected_max > reference_bits;

  if (statistics_simulator.reference_bandwidth_exceeded() !=
      expected_warning) {
    throw std::runtime_error(
        "Incorrect reference bandwidth warning");
  }

  // Round 2: Large messages must still be delivered.
  statistics_simulator.execute_round([&](auto& ctx) {
    const size_t expected =
        G.get_vertex(ctx.id()).out_degree();

    if (ctx.messages().size() != expected) {
      throw std::runtime_error(
          "Large messages were not delivered correctly");
    }

    for (const auto& message : ctx.messages()) {
      const size_t expected_extra =
          message.sender == 0 ? 100 : 0;

      if (message.payload.sender != message.sender ||
          message.payload.extra_bits != expected_extra) {
        throw std::runtime_error(
            "Incorrect large message contents");
      }
    }
  });

  if (statistics_simulator.total_message_bits() != expected_total) {
    throw std::runtime_error(
        "Communication statistics changed unexpectedly");
  }

  if (statistics_simulator.completed_rounds() != 2) {
    throw std::runtime_error(
        "Incorrect statistics-test round count");
  }

  std::cout << "Communication statistics verification passed!\n";

  std::cout << "  Vertices: " << n << "\n";
  std::cout << "  Rounds: "
            << statistics_simulator.completed_rounds() << "\n";
  std::cout << "  Maximum message: "
            << statistics_simulator.max_message_bits()
            << " bits\n";
  std::cout << "  Maximum edge load per round: "
            << statistics_simulator.max_edge_bits_per_round()
            << " bits\n";
  std::cout << "  Total communication: "
            << statistics_simulator.total_message_bits()
            << " bits\n";
  std::cout << "  Reference bandwidth: "
            << statistics_simulator.reference_bandwidth_bits()
            << " bits\n";
  std::cout << "  Reference exceeded: "
            << (statistics_simulator.reference_bandwidth_exceeded()
                    ? "yes" : "no")
            << "\n";

  Simulator<GraphAdapter<Graph>, TestState, TestMessage>
      automatic_simulator(
          adapter,
          test_message_bits,
          CommunicationModel::CONGEST);

  auto counting_algorithm = [](auto& ctx) {
    ++ctx.state().counter;

    if (ctx.state().counter == 3) {
      ctx.terminate();
    }
  };

  bool limit_reached = false;

  try {
    automatic_simulator.run(counting_algorithm, 2);
  } catch (const std::runtime_error&) {
    limit_reached = true;
  }

  if (!limit_reached) {
    throw std::runtime_error(
        "Round limit was not enforced");
  }

  if (automatic_simulator.completed_rounds() != 2) {
    throw std::runtime_error(
        "Incorrect round count after limit");
  }

  if (automatic_simulator.all_terminated()) {
    throw std::runtime_error(
        "Vertices terminated too early");
  }

  for (size_t vertex = 0; vertex < n; ++vertex) {
    const auto id = static_cast<gbbs::uintE>(vertex);

    if (automatic_simulator.state(id).counter != 2) {
      throw std::runtime_error(
          "Vertex state was not preserved");
    }
  }

  automatic_simulator.run(counting_algorithm, 1);

  if (!automatic_simulator.all_terminated()) {
    throw std::runtime_error(
        "Automatic termination failed");
  }

  if (automatic_simulator.completed_rounds() != 3) {
    throw std::runtime_error(
        "Incorrect automatic execution round count");
  }

  const auto& finished_simulator = automatic_simulator;

  for (size_t vertex = 0; vertex < n; ++vertex) {
    const auto id = static_cast<gbbs::uintE>(vertex);

    if (finished_simulator.state(id).counter != 3) {
      throw std::runtime_error(
          "Incorrect final vertex state");
    }

    if (!finished_simulator.is_terminated(id)) {
      throw std::runtime_error(
          "Vertex did not terminate");
    }
  }

  automatic_simulator.run(counting_algorithm, 0);

  if (automatic_simulator.completed_rounds() != 3) {
    throw std::runtime_error(
        "Completed simulation executed additional rounds");
  }

  std::cout << "Automatic execution verification passed!\n";

  return 0.0;
}

}  // namespace distributed

generate_main(distributed::test_node_context, false);
