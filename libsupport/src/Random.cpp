#include "galois/Random.h"
#include "galois/Logging.h"

#include <array>
#include <string>
#include <cstring>
#include <random>
#include <functional>

// https://stackoverflow.com/questions/440133
template <typename T = std::mt19937>
auto random_generator() -> T {
  auto constexpr seed_bits = sizeof(typename T::result_type) * T::state_size;
  auto constexpr seed_len =
      seed_bits / std::numeric_limits<std::seed_seq::result_type>::digits;
  auto seed = std::array<std::seed_seq::result_type, seed_len>{};
  auto dev  = std::random_device{};
  std::generate_n(begin(seed), seed_len, std::ref(dev));
  auto seed_seq = std::seed_seq(begin(seed), end(seed));
  return T{seed_seq};
}

std::string galois::RandomAlphanumericString(uint64_t len) {
  static constexpr auto chars = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  thread_local auto rng = random_generator<>();
  auto dist   = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[dist(rng)]; });
  return result;
}

// Range 0..len-1, inclusive
int64_t galois::RandomUniformInt(int64_t len) {
  thread_local auto rng = random_generator<>();
  GALOIS_LOG_ASSERT(len > 0);
  auto dist = std::uniform_int_distribution{{}, len - 1};
  return dist(rng);
}

// Range min+1..max-1, inclusive
int64_t galois::RandomUniformInt(int64_t min, int64_t max) {
  thread_local auto rng = random_generator<>();
  GALOIS_LOG_ASSERT(min < max);
  auto dist = std::uniform_int_distribution{min + 1, max - 1};
  return dist(rng);
}

// Range 0.0f..max, inclusive
float galois::RandomUniformFloat(float max) {
  thread_local auto rng = random_generator<>();
  std::uniform_real_distribution<float> dist(
      0.0f, std::nextafter(max, std::numeric_limits<float>::max()));
  return dist(rng);
}
