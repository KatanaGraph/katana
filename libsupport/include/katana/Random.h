#ifndef KATANA_LIBSUPPORT_KATANA_RANDOM_H_
#define KATANA_LIBSUPPORT_KATANA_RANDOM_H_

#include <algorithm>
#include <optional>
#include <random>
#include <string>

#include "katana/config.h"

namespace katana {

using RandGenerator = std::mt19937;
using Seed = std::vector<std::seed_seq::result_type>;

/// Generate a random alphanumeric string of length \param len using
/// \param gen if provided. If no generator is specified, use the output of
/// GetGenerator
KATANA_EXPORT std::string RandomAlphanumericString(
    uint64_t len, RandGenerator* gen = nullptr);

/// \returns a random number generator seeded with a user provided seed, or
/// randomness from the platform. The generator is local to the calling thread
/// so uses of it are thread safe. Useful for things like
/// `std::uniform_int_distribution`
KATANA_EXPORT std::pair<katana::RandGenerator, katana::Seed> CreateGenerator(
    const std::optional<Seed>& seed_in);

/// \returns a random number generator obtained from CreateGenerator. This
/// method will store the generator in a thread local variable so multiple calls
/// from the same thread will reuse a previously created generator.
KATANA_EXPORT RandGenerator& GetGenerator(
    const std::optional<Seed>& seed = std::nullopt);

/// Fills the iterator range with  a uniform random sequence of numbers from
/// interval [min_val, max_val]
/// \param start begin iterator
/// \param end end iterator
/// \param min_val inclusive lower bound for random number generation
/// \param max_val inclusive upper bound for random number generation
template <typename ForwardIt, typename T>
void
GenerateUniformRandomSequence(
    const ForwardIt& start, const ForwardIt& end, const T& min_val,
    const T& max_val) {
  using value_type = typename std::iterator_traits<ForwardIt>::value_type;
  static_assert(
      std::is_convertible_v<T, value_type>,
      "Can't convert start_val to iterator's value_type");
  static_assert(
      std::is_arithmetic_v<T> && std::is_arithmetic_v<value_type>,
      "iota only supported for numeric types");

  using DistributionTy = typename std::conditional_t<
      std::is_integral_v<value_type>, std::uniform_int_distribution<value_type>,
      std::uniform_real_distribution<value_type>>;

  DistributionTy distribution(min_val, max_val);

  std::generate(start, end, [&]() -> value_type {
    return distribution(katana::GetGenerator());
  });
}

}  // namespace katana

#endif
