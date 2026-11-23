#pragma once

#include <cstddef>
#include <cstdint>

#include "framework/message_passing/node_context.h"

namespace assignment1 {

// Message types

// You may extend or replace these definitions.
enum class MessageType {
  PRIORITY,
  JOINED_MIS
};

struct Message {
  MessageType type = MessageType::PRIORITY;

  std::uint64_t priority = 0;
};

// Local Vertex State

// You may extend or replace these definitions.
enum class Status {
  ACTIVE,
  IN_MIS,
  NOT_IN_MIS
};

struct State {
  Status status = Status::ACTIVE;

  // TODO (students):
  // Add any local variables needed by your MIS algorithm here.
};

// Communication counting
// Returns the number of bits in the message.
// The function implementation in algorithm.cpp.
size_t message_bits(const Message& message, size_t n);

// Called once per active vertex in every synchronous round.
// You have to implement the randomized MIS algorithm here.
// Available operations include:
//
//   ctx.id()
//   ctx.num_nodes()
//   ctx.state()
//   ctx.messages()
//   ctx.send(neighbor, message)
//   ctx.broadcast(message)
//   ctx.for_each_neighbor(callback)
//   ctx.terminate()
//

template <class Context>
void algorithm(Context& ctx) {
  // TODO (students):
  // Implement randomized MIS using synchronous rounds.

  (void)ctx;
}

}  // namespace assignment1
