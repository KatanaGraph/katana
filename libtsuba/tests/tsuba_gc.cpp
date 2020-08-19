#include <iostream>
#include <vector>
#include <algorithm>

#include "galois/Logging.h"
#include "tsuba/tsuba.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"
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
std::vector<tsuba::RDGMeta> FindVersions(const std::string& src_uri,
                                         uint32_t remaining_versions) {
  auto make_res = tsuba::RDGMeta::Make(src_uri);
  if (!make_res) {
    GALOIS_LOG_FATAL("Cannot open {}: {}", src_uri, make_res.error());
  }

  auto rdg_meta = make_res.value();
  std::vector<tsuba::RDGMeta> versions{};
  versions.push_back(rdg_meta);

  while (rdg_meta.version != rdg_meta.previous_version &&
         versions.size() < remaining_versions) {
    auto rdg_res = GetPreviousRDGMeta(rdg_meta, src_uri);
    if (!rdg_res) {
      // The trail has gone cold
      return versions;
    }
    rdg_meta = rdg_res.value();
    versions.push_back(rdg_meta);
  }
  return versions;
}

void
RDGFileNames(const std::string rdg_uri, std::unordered_set<std::string>& fnames) {
  tsuba::StatBuf stat_buf;
  if(auto res = FileStat(rdg_uri, &stat_buf); !res) {
    // The most recent graph version is stored in a file called
    // meta, not meta_xxxx where xxxx is the version number.
    GALOIS_LOG_DEBUG("File does not exist {}", rdg_uri);
    return;
  }
  auto open_res = tsuba::Open(rdg_uri, tsuba::kReadOnly);
  if(!open_res) {
    GALOIS_LOG_DEBUG("Bad RDG Open {}: {}", rdg_uri, open_res.error());
    return;
  }
  auto rdg_handle = open_res.value();
  auto new_fnames_res = tsuba::FileNames(rdg_handle);
  if(!new_fnames_res) {
    GALOIS_LOG_DEBUG("Bad tsuba::FileNames {}: {}", rdg_uri, new_fnames_res.error());
    return;
  }
  auto new_fnames = new_fnames_res.value();

  fnames.insert(new_fnames.begin(), new_fnames.end());
  auto close_res = tsuba::Close(rdg_handle);
  if(!close_res) {
    GALOIS_LOG_DEBUG("Bad RDG Close {}: {}", rdg_uri, close_res.error());
  }
}

// Collect file names for the given set of graph versions
std::unordered_set<std::string>
GraphFileNames(const std::string& src_uri, const std::vector<tsuba::RDGMeta> metas) {
  std::unordered_set<std::string> fnames{};
  // src_uri == ...meta
  RDGFileNames(src_uri, fnames);
  for(const auto& meta: metas) {
    // src_uri == ...meta_meta.version
    RDGFileNames(tsuba::RDGMeta::FileName(src_uri, meta.version),
                 fnames);
  }
  return fnames;
}

void GC(const std::string& src_uri, uint32_t remaining_versions) {
  auto versions = FindVersions(src_uri, remaining_versions);
  fmt::print("Found versions: ");
  std::for_each(versions.begin(), versions.end(),
                [](const auto& e) { fmt::print("{} ", e.version); });
  fmt::print("\n");

  auto save_listing = GraphFileNames(src_uri, versions);
  fmt::print("Graph paths:\n");
  std::for_each(save_listing.begin(), save_listing.end(),
                [](const auto& e) { fmt::print("{}\n", e); });

  auto res = galois::ExtractDirName(src_uri);
  if (!res) {
    GALOIS_LOG_FATAL("Extracting dir name: {}: {}", src_uri, res.error());
  }
  auto dir      = res.value();
  auto list_res = tsuba::FileListAsync(dir);
  if (!list_res) {
    GALOIS_LOG_FATAL("Bad listing: {}: {}", dir, list_res.error());
  }
  auto faw = std::move(list_res.value());
  while (!faw->Done()) {
    // Get next round of file entries
    if (auto res = (*faw)(); !res) {
      GALOIS_LOG_DEBUG("Bad nested listing call {}", dir);
    }
  }
  auto& listing = faw->GetListingRef();
  fmt::print("All paths:\n");
  std::for_each(listing.begin(), listing.end(),
                [](const auto& e) { fmt::print("{}\n", e); });
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
