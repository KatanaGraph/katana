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
template <typename S, typename T = katana::RandGenerator>
std::pair<T, katana::Seed>
MakeRandomGenerator(S& source) {
  auto constexpr seed_bits = T::word_size * T::state_size;
  auto constexpr seq_bits =
      std::numeric_limits<std::seed_seq::result_type>::digits;
  auto constexpr seed_len = (seed_bits + (seq_bits - 1)) / seq_bits;

  std::array<std::seed_seq::result_type, seed_len> seed;

  std::generate_n(std::begin(seed), seed_len, std::ref(source));

  std::seed_seq seq(std::begin(seed), std::end(seed));
  std::vector<std::seed_seq::result_type> seed_vec(
      std::begin(seed), std::end(seed));
  return std::make_pair(T(seq), std::move(seed_vec));
}

katana::RandGenerator
MakeSystemGenerator() {
  std::random_device dev;
  try {
    return std::get<0>(MakeRandomGenerator(dev));
  } catch (std::exception& e) {
    KATANA_LOG_ERROR(
        "trying alternative random seed method due to error: {}", e.what());
  }

  auto now = std::chrono::system_clock::now();
  auto dur = now.time_since_epoch();
  auto seed = static_cast<katana::RandGenerator::result_type>(dur.count());
  return katana::RandGenerator(seed);
}

thread_local std::unique_ptr<katana::RandGenerator> kRNG;

}  // namespace

katana::RandGenerator&
katana::GetGenerator(const std::optional<katana::Seed>& seed) {
  if (kRNG) {
    return *kRNG;
  }

  auto result = katana::CreateGenerator(seed);
  static std::mutex lock;
  std::lock_guard guard(lock);
  kRNG = std::make_unique<RandGenerator>(std::get<0>(result));

  return *kRNG;
}

std::pair<katana::RandGenerator, katana::Seed>
katana::CreateGenerator(const std::optional<Seed>& seed_in) {
  if (seed_in) {
    std::seed_seq seq(std::begin(*seed_in), std::end(*seed_in));
    return std::make_pair(katana::RandGenerator(seq), *seed_in);
  }

  static RandGenerator system_gen = MakeSystemGenerator();

  return MakeRandomGenerator(system_gen);
}

std::string
katana::RandomAlphanumericString(uint64_t len, RandGenerator* gen) {
  if (gen == nullptr) {
    gen = &GetGenerator();
  }
  static constexpr auto chars =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::uniform_int_distribution dist({}, std::strlen(chars) - 1);
  std::string result(len, '\0');
  std::generate_n(std::begin(result), len, [&]() { return chars[dist(*gen)]; });
  return result;
}
