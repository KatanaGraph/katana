#include "katana/Random.h"

#include <thread>
#include <vector>

#include "katana/Logging.h"

int
main() {
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

  return 0;
}
