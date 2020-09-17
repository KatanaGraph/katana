#include "bench_utils.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

std::string src_uri{};
uint32_t remaining_versions{10};
int opt_verbose_level{0};
bool opt_dry{false};

std::string prog_name = "tsuba_bench";
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

galois::Result<tsuba::RDGMeta>
GetPreviousRDGMeta(const tsuba::RDGMeta& rdg_meta, const std::string& src_uri) {
  auto make_res = tsuba::RDGMeta::Make(src_uri, rdg_meta.previous_version_);
  if (!make_res) {
    GALOIS_LOG_ERROR(
        "Error opening {}: {}\n",
        tsuba::RDGMeta::FileName(src_uri, rdg_meta.previous_version_),
        make_res.error());
  }
  return make_res;
}

// Return a vector of RDGMeta objects, with index 0 being the most recent
// version Vector can have fewer than remaining_versions entries if there aren't
// that many previous versions.
std::vector<tsuba::RDGMeta>
FindVersions(const std::string& src_uri, uint32_t remaining_versions) {
  auto make_res = tsuba::RDGMeta::Make(src_uri);
  if (!make_res) {
    GALOIS_LOG_FATAL("Cannot open {}: {}", src_uri, make_res.error());
  }

  auto rdg_meta = make_res.value();
  std::vector<tsuba::RDGMeta> versions{};
  versions.push_back(rdg_meta);

  while (rdg_meta.version_ != rdg_meta.previous_version_ &&
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
    const std::string rdg_uri, std::unordered_set<std::string>& fnames) {
  tsuba::StatBuf stat_buf;
  if (auto res = FileStat(rdg_uri, &stat_buf); !res) {
    // The most recent graph version is stored in a file called
    // meta, not meta_xxxx where xxxx is the version number.
    GALOIS_LOG_DEBUG("File does not exist {}", rdg_uri);
    return;
  }
  auto open_res = tsuba::Open(rdg_uri, tsuba::kReadOnly);
  if (!open_res) {
    GALOIS_LOG_DEBUG("Bad RDG Open {}: {}", rdg_uri, open_res.error());
    return;
  }
  auto rdg_handle = open_res.value();
  auto new_fnames_res = tsuba::FileNames(rdg_handle);
  if (!new_fnames_res) {
    GALOIS_LOG_DEBUG(
        "Bad tsuba::FileNames {}: {}", rdg_uri, new_fnames_res.error());
    return;
  }
  auto new_fnames = new_fnames_res.value();

  fnames.insert(new_fnames.begin(), new_fnames.end());
  auto close_res = tsuba::Close(rdg_handle);
  if (!close_res) {
    GALOIS_LOG_DEBUG("Bad RDG Close {}: {}", rdg_uri, close_res.error());
  }
}

// Collect file names for the given set of graph versions
std::unordered_set<std::string>
GraphFileNames(
    const std::string& src_uri, const std::vector<tsuba::RDGMeta> metas) {
  std::unordered_set<std::string> fnames{};
  // src_uri == ...meta
  RDGFileNames(src_uri, fnames);
  for (const auto& meta : metas) {
    // src_uri == ...meta_meta.version
    RDGFileNames(tsuba::RDGMeta::FileName(src_uri, meta.version_), fnames);
  }
  return fnames;
}

void
GC(const std::string& src_uri, uint32_t remaining_versions) {
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

  // collect the entire contents of directory into listing
  auto res = galois::ExtractDirName(src_uri);
  if (!res) {
    GALOIS_LOG_FATAL("Extracting dir name: {}: {}", src_uri, res.error());
  }
  auto dir = res.value();
  std::unordered_set<std::string> listing;
  auto list_res = tsuba::FileListAsync(dir, &listing);
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
  if (opt_verbose_level > 0) {
    fmt::print("All  files: {}\n", listing.size());
    if (opt_verbose_level > 1) {
      std::for_each(listing.begin(), listing.end(), [](const auto& e) {
        fmt::print("{}\n", e);
      });
    }
  }
  // Sanity check, all saved files should be in listing
  std::for_each(
      save_listing.begin(), save_listing.end(),
      [&listing, dir](std::string save_file) {
        if (listing.find(save_file) == listing.end()) {
          GALOIS_LOG_FATAL("Save file not in listing: [{}] {}", dir, save_file);
        }
      });
  // Set difference, listing - save_listing
  std::unordered_set<std::string> diff;
  std::copy_if(
      listing.begin(), listing.end(), std::inserter(diff, diff.begin()),
      [&save_listing](std::string list_file) {
        return save_listing.find(list_file) == save_listing.end();
      });

  if (listing.size() - save_listing.size() != diff.size()) {
    GALOIS_LOG_FATAL(
        "Listing: {} Save: {} sub:{} diff:{} ", listing.size(),
        save_listing.size(), listing.size() - save_listing.size(), diff.size());
  }

  // If verbose, output size of files we are deleting
  // This does a lot of calls, so it can be slow
  if (opt_verbose_level > 0) {
    uint64_t size{UINT64_C(0)};
    tsuba::StatBuf stat;
    for (const auto& file : diff) {
      std::string s3path(galois::JoinPath(dir, file));
      auto stat_res = tsuba::FileStat(s3path, &stat);
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
    auto delete_res = tsuba::FileDelete(dir, diff);
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

  GC(src_uri, remaining_versions);

  return 0;
}
