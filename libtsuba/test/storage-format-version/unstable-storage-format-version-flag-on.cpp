#include <filesystem>

#include <boost/filesystem.hpp>

#include "../test-rdg.h"
#include "katana/Experimental.h"
#include "katana/Logging.h"
#include "katana/ProgressTracer.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGStorageFormatVersion.h"
#include "katana/Result.h"
#include "katana/TextTracer.h"
#include "katana/URI.h"

namespace fs = boost::filesystem;

/// Generate a version of the provided stable rdg, marked as unstable for testing
/// the unstable RDG can vary wildly from the stable RDG at test time since features may
/// be under development
/// Because of this, it is possible that this test catches bugs in storage which are related
/// to unstable features, in addition to bugs related to the unstable storage format
/// feature flag itself
///
/// Tests the following while the feature flag is enabled:
/// 1) loading a stable RDG
/// 2) loading a stable RDG and storing it as unstable
/// 3) loading an unstable RDG and storing it as unstable
katana::Result<void>
TestRoundtripUnstable(
    const katana::URI& stable_rdg, const katana::URI& unstable_rdg) {
  KATANA_LOG_ASSERT(!stable_rdg.empty());
  KATANA_LOG_ASSERT(!unstable_rdg.empty());

  // clean up whatever temporary unstable rdg might already be present
  std::filesystem::remove_all(unstable_rdg.path());

  // load a stable rdg
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(stable_rdg));
  // The rdg should not become unstable until it is stored, no matter the state of the `UnstableRDGStorageFormat` flag
  KATANA_LOG_ASSERT(!rdg.IsUnstableStorageFormat());

  // store the unstable rdg
  auto rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg), unstable_rdg));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());
  // ensure where we stored it matches the unstable_rdg path so that the flag-off test can use it
  KATANA_LOG_ASSERT(rdg_dir1 == unstable_rdg);

  // load the unstable rdg
  katana::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));
  KATANA_LOG_ASSERT(rdg1.IsUnstableStorageFormat());

  // RoundTrip it again to ensure we can load an unstable RDG and store it
  auto rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg1)));
  KATANA_LOG_ASSERT(!rdg_dir2.empty());
  katana::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));
  KATANA_LOG_ASSERT(rdg2.IsUnstableStorageFormat());

  return katana::ResultSuccess();
}

katana::Result<void>
Run(const std::string& stable, const std::string& unstable) {
  const katana::URI stable_rdg = KATANA_CHECKED(katana::URI::Make(stable));
  const katana::URI unstable_rdg = KATANA_CHECKED(katana::URI::Make(unstable));

  KATANA_CHECKED(TestRoundtripUnstable(stable_rdg, unstable_rdg));
  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }

  if (argc <= 2) {
    KATANA_LOG_FATAL("missing rdg file directory");
  }
  katana::ProgressTracer::Set(katana::TextTracer::Make());
  katana::ProgressScope host_scope = katana::GetTracer().StartActiveSpan(
      "unstable-storage-format-version-flag-on test");

  // Ensure the feature flag is actually set
  KATANA_LOG_ASSERT(KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat));

  if (auto res = Run(argv[1], argv[2]); !res) {
    KATANA_LOG_FATAL("URI from string {} failed: {}", argv[1], res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
