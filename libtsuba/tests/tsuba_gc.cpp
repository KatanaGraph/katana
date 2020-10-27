#include "bench_utils.h"
#include "galois/Logging.h"
#include "galois/Uri.h"
#include "tsuba/RDG.h"
#include "tsuba/RDG_internal.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

galois::Uri src_uri;
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
  auto uri_res = galois::Uri::Make(argv[index]);
  if (!uri_res) {
    GALOIS_LOG_ERROR("Bad input Uri {}: {}\n", argv[index], uri_res.error());
  }
  src_uri = std::move(uri_res.value());
}

// Get the meta files from a listing
std::vector<uint64_t>
FindMetaVersionsList(
    const std::vector<std::string>& files, uint32_t remaining_versions) {
  std::vector<uint64_t> versions;
  for (const std::string& file : files) {
    if (auto res = tsuba::internal::ParseVersion(file); res) {
      versions.emplace_back(res.value());
    }
  }
  std::sort(std::begin(versions), std::end(versions), [](auto a, auto b) {
    return a > b;
  });

  fmt::print("  Keeping versions: ");
  for (uint32_t i = 0; i < remaining_versions; ++i) {
    if (i < versions.size()) {
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
FindMetaVersionsPtr(const galois::Uri& src_uri, uint32_t remaining_versions) {
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

// Collect file names for the given set of graph versions
std::set<std::string>
GraphFileNames(
    const galois::Uri& src_uri, const std::vector<uint64_t>& versions) {
  std::set<std::string> fnames{};
  for (const auto& version : versions) {
    auto new_fnames_res = tsuba::internal::FileNames(src_uri, version);
    if (!new_fnames_res) {
      GALOIS_LOG_DEBUG(
          "Bad tsuba::FileNames {}: {}", src_uri, new_fnames_res.error());
    }
    auto new_fnames = new_fnames_res.value();
    fnames.insert(new_fnames.begin(), new_fnames.end());
  }
  return fnames;
}

void
ListDir(
    const galois::Uri& dir, std::vector<std::string>* listing,
    std::vector<uint64_t>* size) {
  // collect the entire contents of directory into listing
  listing->clear();
  size->clear();
  auto fut = tsuba::FileListAsync(dir.string(), listing, size);
  GALOIS_LOG_ASSERT(fut.valid());
  if (auto res = fut.get(); !res) {
    GALOIS_LOG_DEBUG("Bad nested listing call {}", dir, res.error());
  }
  if (opt_verbose_level > 0) {
    fmt::print("  All  files: {}\n", listing->size());
    if (opt_verbose_level > 1) {
      std::for_each(listing->cbegin(), listing->cend(), [](const auto& e) {
        fmt::print("{}\n", e);
      });
    }
  }
}

uint64_t
FindSize(
    const std::string& file, const std::vector<std::string>& listing,
    const std::vector<uint64_t>& size) {
  auto itr = std::find(listing.begin(), listing.end(), file);

  if (itr != listing.cend()) {
    auto index = std::distance(listing.begin(), itr);
    return size.at(index);
  }
  return UINT64_C(0);
}

// Sanity check, all saved files should be in listing
void
CheckSavedFilesListed(
    const galois::Uri& src_uri, const std::vector<std::string>& listing,
    const std::set<std::string>& save_listing) {
  // Save time with unordered_set
  // https://medium.com/@gx578007/searching-vector-set-and-unordered-set-6649d1aa7752
  std::unordered_set<std::string> listing_set(listing.begin(), listing.end());
  std::for_each(
      save_listing.begin(), save_listing.end(),
      [&listing_set, src_uri](std::string save_file) {
        if (listing_set.find(save_file) == listing_set.cend()) {
          GALOIS_LOG_FATAL(
              "Save file not in listing: [{}] {}", src_uri, save_file);
        }
      });
}

void
GC(const galois::Uri& src_uri, uint32_t remaining_versions) {
  // collect the entire contents of directory into listing
  std::vector<std::string> listing;
  std::vector<uint64_t> size;
  ListDir(src_uri, &listing, &size);
  auto versions = FindMetaVersionsList(listing, remaining_versions);
  auto save_listing = GraphFileNames(src_uri, versions);
  if (opt_verbose_level > 0) {
    fmt::print("Keep files: {}\n", save_listing.size());
    if (opt_verbose_level > 1) {
      std::for_each(
          save_listing.begin(), save_listing.end(),
          [](const auto& e) { fmt::print("{}\n", e); });
    }
  }

#ifndef NDEBUG
  CheckSavedFilesListed(src_uri, listing, save_listing);
#endif

  // Set difference of std::set listing - save_listing into unordered_set diff
  std::unordered_set<std::string> diff;
  std::set_difference(
      listing.begin(), listing.end(), save_listing.begin(), save_listing.end(),
      std::inserter(diff, diff.begin()));

  // If verbose, output size of files we are deleting
  if (opt_verbose_level > 0) {
    uint64_t total_size{UINT64_C(0)};
    for (const auto& file : diff) {
      total_size += FindSize(file, listing, size);
    }
    auto [scaled_size, units] = BytesToPair(total_size);
    fmt::print(
        "{}Deleting: {} files, {:5.1f}{}\n", opt_dry ? "DRY " : "", diff.size(),
        scaled_size, units);
    if (opt_verbose_level > 1) {
      std::for_each(diff.begin(), diff.end(), [](const auto& e) {
        fmt::print("{}\n", e);
      });
    }
  } else {
    fmt::print("{}Deleting: {} files\n", opt_dry ? "DRY " : "", diff.size());
  }

  // If not a dry run, actually delete
  if (!opt_dry) {
    auto delete_res = tsuba::FileDelete(src_uri.string(), diff);
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
    fmt::print(
        "DRY gc keep {:d} versions from: {}\n", remaining_versions, src_uri);
  } else {
    fmt::print("gc keep {:d} versions from: {}\n", remaining_versions, src_uri);
  }

  GC(src_uri, remaining_versions);

  return 0;
}
