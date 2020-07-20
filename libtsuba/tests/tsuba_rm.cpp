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

std::string usage_msg = "Usage: {} <list of file names>\n";

std::vector<std::string> parse_arguments(int argc, char* argv[]) {
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

  std::vector<std::string> paths{};
  // TODO: Validate paths
  for (auto index = optind; index < argc; ++index) {
    paths.push_back(argv[index]);
  }
  return paths;
}

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  std::vector<std::string> src_paths = parse_arguments(argc, argv);

  // file.h does not support removal/unlink
  GALOIS_LOG_FATAL("Tsuba does not support rm\n");

  for (const auto& path : src_paths) {
    (void)(path);
    // if (auto res = tsuba::FileRm(path, &stat_buf); res != 0) {
    //   GALOIS_LOG_FATAL("\n  Cannot stat {}\n", path);
    // }
    // fmt::print("rm {}\n", path);
  }

  return 0;
}
