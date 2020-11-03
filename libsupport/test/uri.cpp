#include "galois/Uri.h"

#include <vector>

#include "galois/Logging.h"

galois::Uri
Str2Uri(const std::string& str) {
  auto path_res = galois::Uri::Make(str);
  GALOIS_LOG_ASSERT(path_res);
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
    GALOIS_LOG_ASSERT(uri.path() == path);
    GALOIS_LOG_ASSERT(uri.BaseName() == "path");
    GALOIS_LOG_ASSERT(uri.DirName().path() == "/some/long");
  }

  GALOIS_LOG_ASSERT(Str2Uri("path").BaseName() == "path");
  GALOIS_LOG_ASSERT(Str2Uri("path///////").StripSep().path() == "path");

  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long", "path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long/", "path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long", "/path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long//", "path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long///", "path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long///", "/path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::Uri::JoinPath("/some/long///", "//path") == "/some/long/path");

  return 0;
}
