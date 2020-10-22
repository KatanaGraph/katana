#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "galois/Logging.h"
#include "md5.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

uint64_t bytes_to_write{0};
constexpr uint64_t read_block_size = (1 << 29);

std::string usage_msg = "Usage: {} <list of file path>\n";

std::vector<std::string> src_paths;

void
parse_arguments(int argc, char* argv[]) {
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

void
DoMD5(const std::string& path, MD5& md5) {
  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(path, &stat_buf); !res) {
    GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
  }

  std::vector<uint8_t> vec;
  uint64_t size;
  for (uint64_t so_far = UINT64_C(0); so_far < stat_buf.size;
       so_far += read_block_size) {
    size = std::min(read_block_size, (stat_buf.size - so_far));
    vec.reserve(size);
    auto res = tsuba::FileGet(path, vec.data(), so_far, size);
    if (!res) {
      GALOIS_LOG_FATAL(
          "\n  Failed mmap start {:#x} size {:#x} total {:#x}\n", so_far, size,
          stat_buf.size);
    }
    md5.add(vec.data(), size);
  }
}

int
main(int argc, char* argv[]) {
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
