#include "katana/Random.h"

#include <thread>
#include <vector>

#include "katana/Logging.h"

int
main() {
  // test to make sure we have enough randomness
  std::vector<std::thread> threads;
  for (int i = 0; i < 128; ++i) {
    threads.emplace_back([]() {
      std::string s = katana::RandomAlphanumericString(12);
      KATANA_LOG_DEBUG("Got {}", s);
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }
  threads.clear();

  // test to make sure seeded generators are deterministic
  std::vector<katana::RandGenerator> generators;
  generators.reserve(128);
  for (int i = 0; i < 128; ++i) {
    generators.emplace_back(katana::RandGenerator(8675309));
  }

  std::vector<std::string> results(128);
  for (int i = 0; i < 128; ++i) {
    threads.emplace_back([&generators, &results, i]() {
      results[i] = katana::RandomAlphanumericString(12, &generators[i]);
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }

  std::string first_val = results.front();
  for (const auto& val : results) {
    KATANA_LOG_VASSERT(
        val == first_val,
        "seeded rngs should output the same values, "
        "got \"{}\" and \"{}\"",
        first_val, val);
  }

  // Test that CreateGenerator returns a seed that can be used to
  // get deterministic results
  auto [gen1, seed1] = katana::CreateGenerator(std::nullopt);
  auto val1 = katana::RandomAlphanumericString(12, &gen1);
  auto [gen2, seed2] = katana::CreateGenerator(seed1);
  auto val2 = katana::RandomAlphanumericString(12, &gen2);
  KATANA_LOG_VASSERT(
      val1 == val2, "CreateGenerator should return a reusable seed");
  KATANA_LOG_VASSERT(
      seed1 == seed2, "CreateGenerator should return the reused seed");

  return 0;
}
