#include "katana/URI.h"

#include <vector>

#include "katana/Logging.h"

namespace {

katana::Uri
Str2Uri(const std::string& str) {
  auto path_res = katana::Uri::Make(str);
  KATANA_LOG_ASSERT(path_res);
  return path_res.value();
}

void
TestMake() {
  KATANA_LOG_ASSERT(Str2Uri("/some/path/").path() == "/some/path");
  // We only eat one slash by default to support mangled (but valid) paths like
  // this
  KATANA_LOG_ASSERT(Str2Uri("s3:///some/path//").path() == "/some/path/");
  KATANA_LOG_ASSERT(Str2Uri("s3://some/path").path() == "some/path");
  KATANA_LOG_ASSERT(
      Str2Uri("hdfs://somehost:8020/path").path() == "somehost:8020/path");

  KATANA_LOG_ASSERT(Str2Uri("path").BaseName() == "path");
  KATANA_LOG_ASSERT(Str2Uri("path///////").StripSep().path() == "path");
}

void
TestJoinPath() {
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
  KATANA_LOG_ASSERT(
      katana::Uri::JoinPath("/host:8020/long///", "//path") ==
      "/host:8020/long/path");
}

void
TestEncode() {
  // Test that path is not encoded
  KATANA_LOG_ASSERT(Str2Uri("/ with/ spaces").path() == "/ with/ spaces");
  KATANA_LOG_ASSERT(
      Str2Uri("file:///%20with/%20spaces").path() == "/ with/ spaces");

  // Test roundtrip is still a proper URI
  KATANA_LOG_ASSERT(
      Str2Uri("file:///%20with/%20spaces").string() ==
      "file:///%20with/%20spaces");

  // Test that string is encoded
  KATANA_LOG_ASSERT(
      Str2Uri("/ with/ spaces").string() == "file:///%20with/%20spaces");
}

void
TestDecode() {
  KATANA_LOG_ASSERT(katana::Uri::Decode("/ with/ spaces") == "/ with/ spaces");

  KATANA_LOG_ASSERT(
      katana::Uri::Decode("/%20with/%20spaces") == "/ with/ spaces");
  KATANA_LOG_ASSERT(
      katana::Uri::Decode("host%3A8020/path") == "host:8020/path");
}

}  // namespace

int
main() {
  TestMake();

  TestJoinPath();

  TestEncode();

  TestDecode();

  return 0;
}
