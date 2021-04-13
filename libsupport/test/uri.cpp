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
  KATANA_LOG_ASSERT(Str2Uri("/some/path/").path() == "/some/path");
  // We only eat one slash by default to support mangled (but valid) paths like
  // this
  KATANA_LOG_ASSERT(Str2Uri("s3:///some/path//").path() == "/some/path/");
  KATANA_LOG_ASSERT(Str2Uri("s3://some/path").path() == "some/path");

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
