#include <unistd.h>

#include <future>
#include <memory>
#include <vector>
#if __linux__
#include <sys/resource.h>
#include <sys/time.h>
#endif  // __linux__

#include "katana/OSMemoryNotify.h"
#include "katana/Time.h"

void
limit(uint64_t max) {
  struct rlimit rlim;
  // Limit our memory use to so we don't blow out the machine
  rlim.rlim_cur = max;
  rlim.rlim_max = max;
  int res = setrlimit(RLIMIT_AS, &rlim);
  if (res != 0) {
    perror("setrlimit failed");
  }
}

void
SuckMem(uint64_t bytes_to_alloc) {
  std::vector<std::vector<uint64_t>> garbage;
  fmt::print(
      "{} allocating {}\n", std::this_thread::get_id(),
      katana::BytesToStr("{} {}", bytes_to_alloc));
  while (true) {
    std::vector<uint64_t> trash(bytes_to_alloc, 0);
    garbage.push_back(trash);
  }
}

uint64_t
getTotalSystemMemory() {
  uint64_t pages = sysconf(_SC_PHYS_PAGES);
  uint64_t page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

int
main(int, char**) {
#if __linux__
  katana::OSMemoryNotify notify;
  //  int num_threads = 4;
  //  std::vector<std::future<void>> threads;
  uint64_t small_alloc = 1UL << 30;
  uint64_t big_alloc = 1UL << 35;  // Anything larger fails with bad_alloc

  //  limit(16UL << 30);
  uint64_t physical = getTotalSystemMemory();
  uint64_t bytes_to_alloc = small_alloc;
  fmt::print("{} in machine\n", katana::BytesToStr("{} {}", physical));
  if (physical > (1UL << 37)) {
    bytes_to_alloc = big_alloc;
  }
  SuckMem(bytes_to_alloc);

#endif
  return 0;
}
