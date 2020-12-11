#include "galois/Random.h"

#include <thread>
#include <vector>

#include "galois/Logging.h"

int
main() {
  std::vector<std::thread> threads;
  for (int i = 0; i < 128; ++i) {
    threads.emplace_back([]() {
      std::string s = galois::RandomAlphanumericString(12);
      GALOIS_LOG_DEBUG("Got {}", s);
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }

  return 0;
}
