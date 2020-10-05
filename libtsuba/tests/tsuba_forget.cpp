#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "galois/CommBackend.h"
#include "galois/Logging.h"
#include "md5.h"
#include "tsuba/tsuba.h"

uint64_t bytes_to_write{0};
std::string dst_path{};
uint64_t read_block_size = (1 << 29);

std::string usage_msg =
    "Usage: {} <graph_name>...\n"
    "\n"
    "  Remove the named graphs from the namespace, but leave them intact in\n"
    "  storage. User must specify at least one graph\n"
    "\n"
    "Options:\n"
    "  -h  - print this message\n"
    "  -f  - force, return 0 even if the graph was already removed\n";

bool force = false;

std::vector<std::string>
parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "fh")) != -1) {
    switch (c) {
    case 'h':
      fmt::print(stderr, usage_msg, argv[0]);
      exit(0);
      break;
    case 'f':
      force = true;
      break;
    default:
      fmt::print(stderr, usage_msg, argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  if (optind == argc) {
    fmt::print(stderr, "Must provide at least one graph to unlink");
    fmt::print(stderr, usage_msg, argv[0]);
    exit(EXIT_FAILURE);
  }

  return std::vector<std::string>(argv + optind, argv + argc);
}

int
main(int argc, char* argv[]) {
  galois::NullCommBackend comm;
  auto ns_res = tsuba::GetNameServerClient();
  if (!ns_res) {
    GALOIS_LOG_FATAL("tsuba::GetNameServerClient: {}", ns_res.error());
  }
  std::unique_ptr<tsuba::NameServerClient> ns(std::move(ns_res.value()));
  if (auto init_good = tsuba::Init(&comm, ns.get()); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  std::vector<std::string> src_paths = parse_arguments(argc, argv);

  int failed = 0;
  for (const std::string& path : src_paths) {
    if (auto res = tsuba::Forget(path); !res) {
      ++failed;
      if (res.error() == tsuba::ErrorCode::NotFound) {
        if (!force) {
          fmt::print("could not find {}\n", path, res.error());
        }
      } else {
        fmt::print("failed to unlink {}: {}\n", path, res.error());
      }
    }
  }
  if (failed > 0 && !force) {
    return 1;
  }
  return 0;
}
