#include "galois/Uri.h"

#include <libgen.h>

#include <array>
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

// base64 based on https://web.stanford.edu/class/archive/cs/cs106b/cs106b.1186/lectures/08-Fractals/code/expressions/lib/StanfordCPPLib/io/base64.cpp
const std::array<char, 64> kBase64Alphabet{
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

uint64_t
Base64EncodeLen(uint64_t len) {
  return ((len + 2) / 3 * 4);
}

std::string
Base64Encode(std::string_view s) {
  std::string res(Base64EncodeLen(s.length()), '=');
  uint64_t j = 0;
  uint64_t i;
  for (i = 0; i < s.length() - 2; i += 3) {
    res[j++] = kBase64Alphabet.at((s[i] >> 2) & 0x3F);
    res[j++] =
        kBase64Alphabet.at(((s[i] & 0x3) << 4) | ((s[i + 1] & 0xF0) >> 4));
    res[j++] =
        kBase64Alphabet.at(((s[i + 1] & 0xF) << 2) | ((s[i + 2] & 0xC0) >> 6));
    res[j++] = kBase64Alphabet.at(s[i + 2] & 0x3F);
  }
  if (i < s.length()) {
    res[j++] = kBase64Alphabet.at((s[i] >> 2) & 0x3F);
    if (i == (s.length() - 1)) {
      res[j++] = kBase64Alphabet.at(((s[i] & 0x3) << 4));
    } else {
      res[j++] =
          kBase64Alphabet.at(((s[i] & 0x3) << 4) | ((s[i + 1] & 0xF0) >> 4));
      res[j++] = kBase64Alphabet.at(((s[i + 1] & 0xF) << 2));
    }
    // array is initialized to pad character so no need to do that here
  }
  return res;
}

}  // namespace

namespace galois {

Uri::Uri(std::string scheme, std::string path)
    : scheme_(std::move(scheme)),
      path_(std::move(path)),
      string_(scheme_ + "://" + path_) {
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

bool
Uri::empty() const {
  if (scheme_.empty()) {
    assert(path_.empty());
    return true;
  }
  return false;
}

std::string
Uri::Encode() const {
  return Base64Encode(string());
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
