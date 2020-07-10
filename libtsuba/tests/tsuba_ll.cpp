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
std::string dst_path{};
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

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  for (const auto& path : src_paths) {
    tsuba::StatBuf stat_buf;
    if (auto res = tsuba::FileStat(path, &stat_buf); !res) {
      GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
    }
    fmt::print("{} {:#x}\n", path, stat_buf.size);
  }

  return 0;
}
