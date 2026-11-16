#pragma once

#include <cstddef>
#include <limits>
#include <stdexcept>

namespace distributed {

// Returns max(1, ceil(log2(n))).
// This is also the number of bits needed for a vertex ID in [0, n).
inline size_t vertex_id_bits(size_t n) {
  if (n <= 1) {
    return 1;
  }

  size_t bits = 0;
  size_t value = n - 1;

  while (value > 0) {
    ++bits;
    value >>= 1;
  }

  return bits;
}

// Optional CONGEST diagnostic threshold:
// c * max(1, ceil(log2(n))).
//
// c = 0 disables the diagnostic.
inline size_t reference_bandwidth_bits(size_t n, size_t c) {
  if (c == 0) {
    return 0;
  }

  const size_t bits = vertex_id_bits(n);

  if (c > std::numeric_limits<size_t>::max() / bits) {
    throw std::overflow_error("Reference bandwidth overflow");
  }

  return c * bits;
}

}  // namespace distributed
