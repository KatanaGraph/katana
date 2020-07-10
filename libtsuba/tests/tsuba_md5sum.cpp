#include <cerrno>
#include <cstdlib>
#include <string>
#include <unistd.h>

#include <vector>
#include <limits>
#include <numeric>

#include "galois/Logging.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"
#include "md5.h"

uint64_t bytes_to_write{0};
uint64_t read_block_size = (1 << 29);

std::string usage_msg = "Usage: {} <list of file path>\n";

std::vector<std::string> src_paths;

void parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "h")) != -1) {
    switch (c) {
    case 'h':
      fmt::print(stderr, usage_msg, argv[0]);
      exit(0);
      break;
    default:
      fmt::print(stderr, usage_msg, argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  for (auto index = optind; index < argc; ++index) {
    src_paths.push_back(argv[index]);
  }
}

void DoMD5(const std::string& path, MD5& md5) {
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(path, &stat_buf); !res) {
    GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
  }

  uint8_t* buf{nullptr};
  uint64_t size;
  for (uint64_t so_far = 0UL; so_far < stat_buf.size;
       so_far += read_block_size) {
    size     = std::min(read_block_size, (stat_buf.size - so_far));
    auto res = tsuba::FileMmap(path, so_far, size);
    if (!res) {
      GALOIS_LOG_FATAL("\n  Failed mmap start {:#x} size {:#x} total {:#x}\n",
                       so_far, size, stat_buf.size);
    }
    buf = res.value();
    md5.add(buf, size);
    if (auto res = tsuba::FileMunmap(buf); !res) {
      GALOIS_LOG_FATAL("\n  Failed munmap: {}\n", res.error());
    }
  }
}

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("\n  tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  MD5 md5;
  for (const auto& path : src_paths) {
    md5.reset();
    DoMD5(path, md5);
    // md5sum format
    fmt::print("{} *{}\n", md5.getHash(), path);
  }

  return 0;
}
