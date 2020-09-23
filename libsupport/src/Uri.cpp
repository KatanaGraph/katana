#include "galois/Uri.h"

#include <libgen.h>

#include <climits>
#include <cstdlib>
#include <regex>

#include <fmt/format.h>

#include "galois/ErrorCode.h"
#include "galois/FileSystem.h"
#include "galois/Result.h"

namespace {

// get scheme and path, always drop trailing slash
const std::regex kUriRegex("(?:([a-zA-Z0-9]+)://)?(.+)/?");
constexpr const char* kFileScheme = "file";

}  // namespace

namespace galois {

Uri::Uri(std::string scheme, std::string path)
    : scheme_(std::move(scheme)), path_(std::move(path)) {
  assert(!scheme_.empty());
  assert(!path_.empty());
}

Result<Uri>
Uri::MakeFromFile(const std::string& str) {
  std::vector<char> path(PATH_MAX + 1);
  if (realpath(str.c_str(), path.data()) == nullptr) {
    return Uri(kFileScheme, str);
  }
  return Uri(kFileScheme, path.data());
}

Result<Uri>
Uri::Make(const std::string& str) {
  std::smatch sub_match;
  if (!std::regex_match(str, sub_match, kUriRegex)) {
    return ErrorCode::InvalidArgument;
  }
  std::string scheme(sub_match[1]);
  std::string path(sub_match[2]);
  if (scheme.empty()) {
    return MakeFromFile(path);
  }
  return Uri(scheme, path);
}

std::string
Uri::BaseName() const {
  return ExtractFileName(path());
}

Uri
Uri::DirName() const {
  auto dir_res = ExtractDirName(path());
  if (!dir_res) {
    return Uri();
  }
  return Uri(scheme_, dir_res.value());
}

Uri
Uri::Append(std::string_view to_append) const {
  if (empty()) {
    return Uri();
  }
  return Uri(scheme_, JoinPath(path_, to_append));
}

Uri
Uri::RandFile(std::string_view prefix) const {
  if (empty()) {
    return Uri();
  }
  return Uri(scheme_, NewPath(path_, prefix));
}

bool
operator==(const Uri& lhs, const Uri& rhs) {
  return (lhs.scheme() == rhs.scheme()) && (lhs.path() == rhs.path());
}

bool
operator!=(const Uri& lhs, const Uri& rhs) {
  return !(lhs == rhs);
}

}  // namespace galois
