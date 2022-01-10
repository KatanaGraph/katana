#include <filesystem>

#include <boost/filesystem.hpp>

#include "../test-rdg.h"
#include "katana/Experimental.h"
#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGStorageFormatVersion.h"
#include "katana/Result.h"
#include "katana/URI.h"

namespace fs = boost::filesystem;

/// Tests the following while the feature flag is disabled:
/// 1) loading and storing a stable RDG
katana::Result<void>
TestStable(const std::string& stable_rdg) {
  KATANA_LOG_ASSERT(!stable_rdg.empty());

  // load a stable rdg
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(stable_rdg));
  KATANA_LOG_ASSERT(!rdg.IsUnstableStorageFormat());

  // store the stable rdg
  std::string rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  return katana::ResultSuccess();
}

/// Tests the following while the feature flag is disabled:
/// 1) loading an unstable RDG
katana::Result<void>
TestLoadUnstable(const std::string& unstable_rdg) {
  KATANA_LOG_ASSERT(!unstable_rdg.empty());

  // this should fail
  // Can't use KATANA_CHECKED since we want failure
  auto res = LoadRDG(unstable_rdg);
  KATANA_LOG_ASSERT(!res);

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

  // Ensure the feature flag is not set
  KATANA_LOG_ASSERT(!KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat));

  const std::string& stable_rdg = argv[1];
  const std::string& unstable_rdg = argv[2];

  auto res = TestStable(stable_rdg);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  res = TestLoadUnstable(unstable_rdg);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
