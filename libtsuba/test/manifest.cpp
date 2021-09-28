#include <boost/filesystem.hpp>

#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/RDG.h"
#include "tsuba/RDGManifest.h"

namespace fs = boost::filesystem;

katana::Result<void>
TestFileNames(const std::string& path) {
  auto uri = KATANA_CHECKED(katana::Uri::MakeFromFile(path));
  auto manifest = KATANA_CHECKED(tsuba::RDGManifest::Make(uri));

  fs::path p = path;
  p = p.parent_path();

  std::set<std::string> names = KATANA_CHECKED(manifest.FileNames());
  for (const auto& name : names) {
    fs::path n = p / name;

    if (!fs::exists(n) || !fs::is_regular(n)) {
      return KATANA_ERROR(
          katana::ErrorCode::AssertionFailed,
          "path {} does not exist or is not a regular file",
          std::quoted(n.string()));
    }
  }

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll(const std::string& path) {
  KATANA_CHECKED_CONTEXT(TestFileNames(path), "TestFileNames");

  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    KATANA_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  if (argc <= 1) {
    KATANA_LOG_FATAL("manifest <rdg prefix>");
  }

  auto res = TestAll(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = tsuba::Fini(); !fini_good) {
    KATANA_LOG_FATAL("tsuba::Fini: {}", fini_good.error());
  }

  return 0;
}
