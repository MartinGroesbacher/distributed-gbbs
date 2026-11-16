#include "assignment1/algorithm.h"

namespace assignment1 {

size_t message_bits(const Message& message, size_t n) {
  (void)n;

  switch (message.type) {
    case MessageType::PRIORITY:
      // One type bit and one 64-bit priority.
      return 65;

    case MessageType::JOINED_MIS:
      // One type bit.
      return 1;
  }

  return 0;
}

}  // namespace assignment1
