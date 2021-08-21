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

  return 0;
}
