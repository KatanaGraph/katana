#include "katana/Random.h"

#include <array>
#include <chrono>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <utility>

#include "katana/Logging.h"

namespace {

// Generating good random numbers portably is not guaranteed. Balance between
// maximizing entropy without stopping callers.
//
// Design points:
// 1. Keep system dependence independent of the number of threads
// 2. Trade off entropy for availability because we don't need
//    cryptographically strong randomness
//
// So, seed a system RNG up to its state size using the portable and
// best-effort std::random_device. Then, use this generator to seed per thread
// generators. If we cannot seed our system RNG with the total state size, fall
// back to grabbing some bits from the clock instead.
//
// Note that there will be some bias even if the system generator is
// initialized perfectly due to bias in how seed_seq works [1].
//
// [1] https://www.pcg-random.org/posts/cpp-seeding-surprises.html

// https://stackoverflow.com/questions/440133
template <typename S, typename T = std::mt19937>
T
MakeRandomGenerator(S& source) {
  auto constexpr seed_bits = T::word_size * T::state_size;
  auto constexpr seq_bits =
      std::numeric_limits<std::seed_seq::result_type>::digits;
  auto constexpr seed_len = (seed_bits + (seq_bits - 1)) / seq_bits;

  std::array<std::seed_seq::result_type, seed_len> seed;

  std::generate_n(std::begin(seed), seed_len, std::ref(source));

  std::seed_seq seq(std::begin(seed), std::end(seed));
  return T(seq);
}

std::mt19937
MakeSystemGenerator() {
  std::random_device dev;
  try {
    return MakeRandomGenerator(dev);
  } catch (std::exception& e) {
    KATANA_LOG_ERROR(
        "trying alternative random seed method due to error: {}", e.what());
  }

  auto now = std::chrono::system_clock::now();
  auto dur = now.time_since_epoch();
  return std::mt19937(dur.count());
}

thread_local std::unique_ptr<std::mt19937> kRNG;

}  // namespace

std::mt19937&
katana::GetGenerator() {
  if (kRNG) {
    return *kRNG;
  }

  static std::mutex lock;
  static std::mt19937 system_gen = MakeSystemGenerator();
  std::lock_guard guard(lock);

  kRNG = std::make_unique<std::mt19937>(MakeRandomGenerator(system_gen));

  return *kRNG;
}

std::string
katana::RandomAlphanumericString(uint64_t len) {
  static constexpr auto chars =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::uniform_int_distribution dist({}, std::strlen(chars) - 1);
  std::string result(len, '\0');
  std::generate_n(
      std::begin(result), len, [&]() { return chars[dist(GetGenerator())]; });
  return result;
}

// Range 0..len-1, inclusive
int64_t
katana::RandomUniformInt(int64_t len) {
  KATANA_LOG_ASSERT(len > 0);
  std::uniform_int_distribution dist({}, len - 1);
  return dist(GetGenerator());
}

// Range min+1..max-1, inclusive
int64_t
katana::RandomUniformInt(int64_t min, int64_t max) {
  KATANA_LOG_ASSERT(min < max);
  std::uniform_int_distribution dist(min + 1, max - 1);
  return dist(GetGenerator());
}

// Range 0.0f..max, inclusive
float
katana::RandomUniformFloat(float max) {
  std::uniform_real_distribution<float> dist(
      0.0f, std::nextafter(max, std::numeric_limits<float>::max()));
  return dist(GetGenerator());
}
