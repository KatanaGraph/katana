#include <iostream>
#include <vector>
#include <algorithm>

#include "galois/Logging.h"
#include "tsuba/tsuba.h"
#include "tsuba/RDG.h"
#include "galois/FileSystem.h"

std::string src_uri{};
uint32_t remaining_versions{10};

std::string usage_msg = "Usage: {} <RDG URI>\n"
                        "  [-r] remaining versions (default=10)\n"
                        "  [-h] usage message\n";

void parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "hr:")) != -1) {
    switch (c) {
    case 'r':
      remaining_versions = std::atoi(optarg);
      break;
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
  GALOIS_LOG_VASSERT(index < argc, "{} requires property graph URI", argv[0]);
  src_uri = argv[index++];
}

galois::Result<tsuba::RDGMeta> GetPreviousRDGMeta(tsuba::RDGMeta rdg_meta,
                                                  const std::string& src_uri) {
  auto fn       = tsuba::RDGMeta::FileName(src_uri, rdg_meta.previous_version);
  auto make_res = tsuba::RDGMeta::Make(fn);
  if (!make_res) {
    fmt::print("Error opening {}: {}\n", fn, make_res.error());
    return make_res.error();
  }
  return make_res.value();
}

// Return a vector of valid version numbers, with index 0 being the most recent
// version Vector can have fewer than remaining_versions entries if there aren't
// that many previous versions.
std::vector<uint64_t> FindVersions(const std::string& src_uri,
                                   uint32_t remaining_versions) {
  auto make_res = tsuba::RDGMeta::Make(src_uri);
  if (!make_res) {
    GALOIS_LOG_FATAL("Cannot open {}: {}", src_uri, make_res.error());
  }

  tsuba::RDGMeta rdg_meta = make_res.value();
  std::vector<uint64_t> versions{};
  versions.push_back(rdg_meta.version);

  while (rdg_meta.version != rdg_meta.previous_version &&
         versions.size() < remaining_versions) {
    auto rdg_res = GetPreviousRDGMeta(rdg_meta, src_uri);
    if (!rdg_res) {
      // The trail has gone cold
      return versions;
    }
    rdg_meta = rdg_res.value();
    versions.push_back(rdg_meta.version);
  }
  return versions;
}

void GC(const std::string& src_uri, uint32_t remaining_versions) {
  std::vector<uint64_t> versions = FindVersions(src_uri, remaining_versions);
  std::for_each(versions.begin(), versions.end(),
                [](const auto& e) { fmt::print("{} ", e); });
  fmt::print("\n");
}

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  fmt::print("gc count {:d}: {}\n", remaining_versions, src_uri);

  GC(src_uri, remaining_versions);

  return 0;
}
