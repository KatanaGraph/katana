#include <boost/filesystem.hpp>

#include "katana/FileView.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/file.h"
#include "katana/tsuba.h"

namespace fs = boost::filesystem;

katana::Result<void>
TestEmpty(const std::string& path) {
  if (boost::system::error_code err; !fs::create_directories(path, err)) {
    if (err) {
      return KATANA_ERROR(
          std::error_code(err.value(), err.category()),
          "creating parent directories: {}", err.message());
    }
  }

  auto uri = KATANA_CHECKED(katana::URI::MakeFromFile(path));
  auto empty_uri = uri.Join("empty_file");

  KATANA_CHECKED(katana::FileStore(empty_uri.string(), std::string("")));
  katana::FileView fv;

  KATANA_CHECKED(fv.Bind(empty_uri.string(), true));

  KATANA_LOG_ASSERT(fv.size() == 0);

  return katana::ResultSuccess();
}

katana::Result<void>
TestAll(const std::string& path) {
  KATANA_CHECKED_CONTEXT(TestEmpty(path), "TestEmpty");

  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }

  if (argc <= 1) {
    KATANA_LOG_FATAL("{} <empty dir>", argv[0]);
  }

  auto res = TestAll(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
