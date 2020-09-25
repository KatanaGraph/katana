#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "tsuba/RDG.h"
#include "tsuba/RDG_internal.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

std::string src_uri{};
uint32_t remaining_versions{10};
int opt_verbose_level{0};
bool opt_dry{false};

std::string prog_name = "tsuba_gc";
std::string usage_msg =
    "Usage: {} <RDG URI>\n"
    "  [-r] remaining versions (default=10)\n"
    "  [-n] dry run (default=false)\n"
    "  [-v] verbose, can be repeated (default=false)\n"
    "  [-h] usage message\n";

void
parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "hnr:v")) != -1) {
    switch (c) {
    case 'r':
      remaining_versions = std::atoi(optarg);
      break;
    case 'n':
      opt_dry = true;
      break;
    case 'v':
      opt_verbose_level++;
      break;
    case 'h':
      fmt::print(stderr, usage_msg, prog_name);
      exit(0);
      break;
    default:
      fmt::print(stderr, usage_msg, prog_name);
      exit(EXIT_FAILURE);
    }
  }

  // TODO: Validate paths
  auto index = optind;
  if (index >= argc) {
    fmt::print(stderr, "{} requires property graph URI argument\n", prog_name);
    exit(EXIT_FAILURE);
  }
  src_uri = argv[index++];
}

// Get the meta files from a listing
std::vector<uint64_t>
FindMetaVersionsLs(
    const std::unordered_set<std::string>& files, uint32_t remaining_versions) {
  std::vector<uint64_t> versions;
  for (const std::string& file : files) {
    if (auto res = tsuba::internal::ParseVersion(file); res) {
      versions.emplace_back(res.value());
    }
  }
  std::sort(std::begin(versions), std::end(versions), [](auto a, auto b) {
    return a > b;
  });

  fmt::print("Keeping versions: ");
  for (uint32_t i = 0; i < remaining_versions; ++i) {
    if(i < versions.size()) {
      fmt::print("{} ", versions.at(i));
    }
  }
  fmt::print("\n");
  versions.resize(remaining_versions);
  return versions;
}

galois::Result<tsuba::RDGMeta>
GetPreviousRDGMeta(const tsuba::RDGMeta& rdg_meta, const galois::Uri& src_uri) {
  auto make_res = tsuba::RDGMeta::Make(src_uri, rdg_meta.previous_version_);
  if (!make_res) {
    GALOIS_LOG_ERROR(
        "Error opening {}: {}\n",
        tsuba::RDGMeta::FileName(src_uri, rdg_meta.previous_version_),
        make_res.error());
  }
  return make_res;
}

// Get the meta files by following pointers
// Return a vector of RDGMeta objects, with index 0 being the most recent
// version Vector can have fewer than remaining_versions entries if there aren't
// that many previous versions.
std::vector<tsuba::RDGMeta>
FindVersions(const galois::Uri& src_uri, uint32_t remaining_versions) {
  auto make_res = tsuba::RDGMeta::Make(src_uri);
  if (!make_res) {
    GALOIS_LOG_FATAL("Cannot open {}: {}", src_uri, make_res.error());
  }

  auto rdg_meta = make_res.value();
  std::vector<tsuba::RDGMeta> versions{};
  versions.push_back(rdg_meta);

  while (rdg_meta.version_ > 1 &&
         rdg_meta.version_ != rdg_meta.previous_version_ &&
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
RDGFileNames(
    const galois::Uri rdg_dir, uint64_t version,
    std::unordered_set<std::string>& fnames) {
  auto new_fnames_res = tsuba::internal::FileNames(rdg_dir, version);
  if (!new_fnames_res) {
    GALOIS_LOG_DEBUG(
        "Bad tsuba::FileNames {}: {}", rdg_dir, new_fnames_res.error());
    return;
  }
  auto new_fnames = new_fnames_res.value();
  fnames.insert(new_fnames.begin(), new_fnames.end());
}

// Collect file names for the given set of graph versions
std::unordered_set<std::string>
GraphFileNames(
    const galois::Uri& src_uri, const std::vector<tsuba::RDGMeta>& metas) {
  std::unordered_set<std::string> fnames{};
  for (const auto& meta : metas) {
    // src_uri == ...meta_meta.version
    RDGFileNames(rdg_dir, meta.version_, fnames);
  }
  return fnames;
}

void
GC(const galois::Uri& src_uri, uint32_t remaining_versions) {
  auto versions = FindVersions(src_uri, remaining_versions);
  fmt::print("Keeping versions: ");
  std::for_each(versions.begin(), versions.end(), [](const auto& e) {
    fmt::print("{} ", e.version_);
  });
  fmt::print("\n");

  auto save_listing = GraphFileNames(src_uri, versions);
  if (opt_verbose_level > 0) {
    fmt::print("Keep files: {}\n", save_listing.size());
    if (opt_verbose_level > 1) {
      std::for_each(
          save_listing.begin(), save_listing.end(),
          [](const auto& e) { fmt::print("{}\n", e); });
    }
  }
}

std::unordered_set<std::string>
ListDir(const std::string& src_dir) {
  // collect the entire contents of directory into listing
  galois::Uri dir = src_uri.DirName();
  std::unordered_set<std::string> listing;
  auto list_res = tsuba::FileListAsync(dir.string(), &listing);
  if (!list_res) {
    GALOIS_LOG_ERROR("Bad listing: {}: {}", src_dir, list_res.error());
    return listing;
  }
  auto faw = std::move(list_res.value());
  while (faw != nullptr && !faw->Done()) {
    // Get next round of file entries
    if (auto res = (*faw)(); !res) {
      GALOIS_LOG_DEBUG("Bad nested listing call {}", src_dir);
    }
  }
  if (opt_verbose_level > 0) {
    fmt::print("All  files: {}\n", listing.size());
    if (opt_verbose_level > 1) {
      std::for_each(listing.begin(), listing.end(), [](const auto& e) {
        fmt::print("{}\n", e);
      });
    }
  }
  return listing;
}

// Set difference
std::unordered_set<std::string>
SetDiff(std::unordered_set<std::string> a, std::unordered_set<std::string> b) {
  std::unordered_set<std::string> diff;
  std::copy_if(
      a.begin(), a.end(), std::inserter(diff, diff.begin()),
      [&b](std::string list_file) { return b.find(list_file) == b.end(); });

  if (a.size() - b.size() != diff.size()) {
    GALOIS_LOG_FATAL(
        "Listing: {} Save: {} sub: {} diff: {} ", a.size(), b.size(),
        a.size() - b.size(), diff.size());
  }
  return diff;
}

void
GC(const std::string& src_uri, uint32_t remaining_versions) {
  // collect the entire contents of directory into listing
  auto listing = ListDir(src_uri);
  auto versions = FindMetaVersionsLs(listing, remaining_versions);
  auto save_listing = GraphFileNames(src_uri, versions);
  if (opt_verbose_level > 0) {
    fmt::print("Keep files: {}\n", save_listing.size());
    if (opt_verbose_level > 1) {
      std::for_each(
          save_listing.begin(), save_listing.end(),
          [](const auto& e) { fmt::print("{}\n", e); });
    }
  }

  // Sanity check, all saved files should be in listing
  std::for_each(
      save_listing.begin(), save_listing.end(),
      [&listing, src_uri](std::string save_file) {
        if (listing.find(save_file) == listing.end()) {
          GALOIS_LOG_FATAL(
              "Save file not in listing: [{}] {}", src_uri, save_file);
        }
      });

  // Set difference, listing - save_listing
  std::unordered_set<std::string> diff = SetDiff(listing, save_listing);

  // If verbose, output size of files we are deleting
  // This does a lot of calls, so it can be slow
  if (opt_verbose_level > 0) {
    uint64_t size{UINT64_C(0)};
    tsuba::StatBuf stat;
    for (const auto& file : diff) {
      if (!stat_res) {
        GALOIS_LOG_DEBUG("Bad GC delete {}: {}", src_uri, stat_res.error());
      } else {
        size += stat.size;
      }
    }
    auto [scaled_size, units] = BytesToPair(size);
    fmt::print(
        "Deleting: {} files, {:5.1f}{}\n", diff.size(), scaled_size, units);
  } else {
    fmt::print("Deleting: {} files\n", diff.size());
  }

  // If not a dry run, actually delete
  if (!opt_dry) {
    auto delete_res = tsuba::FileDelete(dir.string(), diff);
    if (!delete_res) {
      GALOIS_LOG_DEBUG("Bad GC delete {}: {}", src_uri, delete_res.error());
    }
  }
}

int
main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  if (opt_dry) {
    fmt::print("DRY gc count {:d}: {}\n", remaining_versions, src_uri);
  } else {
    fmt::print("gc count {:d}: {}\n", remaining_versions, src_uri);
  }

  auto uri_res = galois::Uri::Make(src_uri);
  if (!uri_res) {
    GALOIS_LOG_FATAL(
        "does not look like a uri ({}): {}", src_uri, uri_res.error());
  }
  galois::Uri uri = std::move(uri_res.value());
  GC(uri, remaining_versions);

  return 0;
}
