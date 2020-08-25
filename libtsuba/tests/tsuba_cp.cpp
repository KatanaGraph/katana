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

std::string src_path{};
std::string dst_path{};
uint64_t read_block_size = (1 << 29);

std::string usage_msg = "Usage: {} <src file name> <dst file name>\n";

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

  // TODO: Validate paths
  auto index = optind;
  src_path   = argv[index++];
  dst_path   = argv[index++];
}

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  // S3 does not allow appends, so we must read entire file into memory, then
  // write.

  tsuba::StatBuf stat_buf;
  if (auto res = tsuba::FileStat(src_path, &stat_buf); !res) {
    GALOIS_LOG_FATAL("Cannot stat {}\n", src_path);
  }

  fmt::print("cp {} to {}\n", src_path, dst_path);

  auto buf_res = tsuba::FileMmap(src_path, UINT64_C(0), stat_buf.size);
  if (!buf_res) {
    GALOIS_LOG_FATAL("Failed mmap start 0 size {:#x}\n", stat_buf.size);
  }
  uint8_t* buf = buf_res.value();

  if (auto res = tsuba::FileStore(dst_path, buf, stat_buf.size); !res) {
    fmt::print(stderr, "FileStore error {}\n", res.error());
    exit(EXIT_FAILURE);
  }

  return 0;
}
