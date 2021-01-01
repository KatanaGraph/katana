#include "katana/Uri.h"

#include <vector>

#include "katana/Logging.h"

katana::Uri
Str2Uri(const std::string& str) {
  auto path_res = katana::Uri::Make(str);
  KATANA_LOG_ASSERT(path_res);
  return path_res.value();
}

int
main() {
  const std::vector<std::string> all_paths = {
      "/some/long/path",
      "/some/long/path/",
      "/some/long/path//",
      "/some/long/path///",
  };
  for (const auto& path : all_paths) {
    auto uri = Str2Uri(path);
    KATANA_LOG_ASSERT(uri.path() == path);
    KATANA_LOG_ASSERT(uri.BaseName() == "path");
    KATANA_LOG_ASSERT(uri.DirName().path() == "/some/long");
  }

  KATANA_LOG_ASSERT(Str2Uri("path").BaseName() == "path");
  KATANA_LOG_ASSERT(Str2Uri("path///////").StripSep().path() == "path");

  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long", "path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long/", "path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long", "/path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long//", "path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long///", "path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long///", "/path") == "/some/long/path");
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/some/long///", "//path") == "/some/long/path");

  return 0;
}
